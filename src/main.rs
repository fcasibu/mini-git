use anyhow::{Context, Result, anyhow};
use bincode::{self, Decode, Encode, config};
use clap::{Parser, Subcommand};
use flate2::Compression;
use flate2::write::{ZlibDecoder, ZlibEncoder};
use hex::encode;
use sha1::{Digest, Sha1};
use std::{
    env, fs,
    io::{self, Read, Write},
    path::{Path, PathBuf},
};

struct BlobObject {
    hash: [u8; 20],
    compressed_content: Vec<u8>,
    raw_content: String,
}

impl BlobObject {
    pub fn new(raw_content: &str) -> Result<Self> {
        let raw_bytes = raw_content.as_bytes();
        let header = format!("blob {}\0", raw_bytes.len());

        let mut object_content = Vec::new();
        object_content.extend_from_slice(header.as_bytes());
        object_content.extend_from_slice(raw_bytes);

        let hash = hash_content(&object_content);
        let compressed_content =
            compress_content(&object_content).context("Failed to compress blob content")?;

        Ok(BlobObject {
            hash,
            compressed_content,
            raw_content: raw_content.to_string(),
        })
    }
}

struct TreeObject {
    hash: [u8; 20],
    compressed_content: Vec<u8>,
    raw_content: Vec<u8>,
}

impl TreeObject {
    pub fn new(entries: &[IndexEntry]) -> Result<Self> {
        let mut raw_content = Vec::new();

        for entry in entries {
            raw_content.extend_from_slice(entry.mode.to_string().as_bytes());
            raw_content.push(b' ');
            raw_content.extend_from_slice(entry.path.to_string_lossy().as_bytes());
            raw_content.push(0);

            raw_content.extend_from_slice(&entry.sha1);
        }

        let header = format!("tree {}\0", raw_content.len());
        let mut full_content = header.as_bytes().to_vec();
        full_content.extend_from_slice(&raw_content);

        let hash = hash_content(&full_content);
        let compressed_content = compress_content(&full_content)?;

        Ok(TreeObject {
            hash,
            compressed_content,
            raw_content,
        })
    }
}

enum GitObjects {
    Blob(BlobObject),
    Tree(TreeObject),
}

struct Repository {
    objects_dir: PathBuf,
    mini_git_dir: PathBuf,
    index_file: PathBuf,
}

impl Repository {
    pub fn new() -> Result<Self> {
        let mini_git_dir = env::current_dir()?.join(".mini-git");
        let objects_dir = mini_git_dir.join("objects");
        let index_file = mini_git_dir.join("index");

        Ok(Repository {
            objects_dir,
            mini_git_dir,
            index_file,
        })
    }

    pub fn init(&self) -> Result<()> {
        let mini_git_dir = &self.mini_git_dir;
        let objects_dir = &self.objects_dir;

        if mini_git_dir.exists() {
            println!(
                "Reinitialized existing MiniGit repository in {}",
                mini_git_dir.display()
            );
        } else {
            fs::create_dir(&mini_git_dir).with_context(|| {
                format!(
                    "Failed to create .mini-git directory at {}",
                    mini_git_dir.display()
                )
            })?;
            println!(
                "Initialized empty MiniGit repository in {}",
                mini_git_dir.display()
            );
        }

        fs::create_dir_all(&objects_dir).context("Failed to create objects directory")?;
        fs::create_dir_all(mini_git_dir.join("refs").join("heads"))
            .context("Failed to create refs/heads directory")?;
        fs::create_dir_all(mini_git_dir.join("refs").join("tags"))
            .context("Failed to create refs/tags directory")?;

        let head_file = mini_git_dir.join("HEAD");
        if !head_file.exists() {
            fs::write(&head_file, "ref: refs/heads/main\n")
                .with_context(|| format!("Failed to write HEAD file at {}", head_file.display()))?;
        }

        let index_file = mini_git_dir.join("index");
        if !index_file.exists() {
            fs::write(&index_file, "").with_context(|| {
                format!("Failed to write index file at {}", index_file.display())
            })?;
        }

        Ok(())
    }

    pub fn write_object(&self, input_data: Option<&str>) -> Result<([u8; 20], String)> {
        let objects_dir = &self.objects_dir;

        if !objects_dir.is_dir() {
            return Err(anyhow!(
                "fatal: not a mini-git repository (or any of the parent directories): .mini-git"
            ));
        }

        let (compressed_content, sha1, encoded_hash) = match input_data {
            Some(data) => {
                let blob_object = BlobObject::new(&data)?;

                (
                    blob_object.compressed_content,
                    blob_object.hash,
                    encode(blob_object.hash),
                )
            }
            None => {
                let index = self.read_index()?;
                let tree_object = TreeObject::new(&index.entries)?;

                (
                    tree_object.compressed_content,
                    tree_object.hash,
                    encode(tree_object.hash),
                )
            }
        };

        let (dir_prefix, file_suffix) = encoded_hash.split_at(2);
        let object_subdir = objects_dir.join(dir_prefix);
        let object_file_path = object_subdir.join(file_suffix);

        if !object_subdir.exists() {
            fs::create_dir(&object_subdir).with_context(|| {
                format!(
                    "Failed to create object subdirectory {}",
                    object_subdir.display()
                )
            })?;
        }

        fs::write(&object_file_path, &compressed_content).with_context(|| {
            format!("Failed to write object file {}", object_file_path.display())
        })?;

        Ok((sha1, encoded_hash))
    }

    pub fn add_to_index(&self, file_path: &PathBuf) -> Result<()> {
        let index_file = &self.index_file;
        let file_path_buf = file_path.to_path_buf();

        if !index_file.is_file() {
            return Err(anyhow!(
                "fatal: not a mini-git repository (or any of the parent directories): .mini-git"
            ));
        }

        if !file_path.exists() {
            return Err(anyhow!("Failed to read {:?}", file_path));
        }

        let data = fs::read_to_string(&file_path)
            .with_context(|| format!("Failed to read file {}", file_path.display()))?;

        let (sha1, _) = self.write_object(Some(&data))?;

        let mut index = self.read_index()?;

        if let Some(pos) = index.entries.iter().position(|e| e.path == file_path_buf) {
            if index.entries[pos].sha1 != sha1 {
                index.entries[pos] = IndexEntry {
                    mode: 100644,
                    sha1,
                    path: file_path_buf,
                };
            }
        } else {
            index.entries.push(IndexEntry {
                mode: 100644,
                sha1,
                path: file_path_buf,
            });
        }

        index.entries.sort();

        fs::write(
            &index_file,
            bincode::encode_to_vec(&index, config::standard())?,
        )
        .with_context(|| format!("Failed to write index file"))?;

        Ok(())
    }

    pub fn read_index(&self) -> Result<IndexFile> {
        let index_file = &self.index_file;

        if !index_file.is_file() {
            return Err(anyhow!(
                "fatal: not a mini-git repository (or any of the parent directories): .mini-git"
            ));
        }

        let index_data = fs::read(&index_file)
            .with_context(|| format!("Failed to read index file {}", index_file.display()))?;

        if index_data.is_empty() {
            return Ok(IndexFile {
                entries: Vec::new(),
            });
        }

        let (index, _): (IndexFile, usize) =
            bincode::decode_from_slice(&index_data, config::standard())
                .context("Failed to decode index file")?;

        Ok(index)
    }

    pub fn read_object(&self, object_hash_str: &str) -> Result<GitObjects> {
        let objects_dir = &self.objects_dir;

        if !objects_dir.is_dir() {
            return Err(anyhow!(
                "fatal: not a mini-git repository (or any of the parent directories): .mini-git"
            ));
        }

        let object_file_path = self.get_object_path(&object_hash_str)?;

        if !object_file_path.exists() {
            return Err(anyhow!("fatal: object {} does not exist", object_hash_str));
        }

        let compressed_data = fs::read(&object_file_path).with_context(|| {
            format!("Failed to read object file {}", object_file_path.display())
        })?;

        let decompressed = decompress_content(&compressed_data)?;

        let space = decompressed.iter().position(|&b| b == b' ').unwrap();
        let null_terminator_position = decompressed.iter().position(|&b| b == 0).unwrap();

        let object_type = std::str::from_utf8(&decompressed[0..space])?;
        let content = &decompressed[null_terminator_position + 1..];

        match object_type {
            "blob" => Ok(GitObjects::Blob(BlobObject::new(&String::from_utf8(
                content.to_vec(),
            )?)?)),
            "tree" => {
                // TODO(fcasibu): Should parse content instead of reading from the index
                let index = self.read_index()?;
                Ok(GitObjects::Tree(TreeObject::new(&index.entries)?))
            }
            _ => Err(anyhow!(
                "Object type \"{}\" not yet implemented",
                object_type
            )),
        }
    }

    pub fn write_tree(&self) -> Result<([u8; 20], String)> {
        let objects_dir = &self.objects_dir;

        if !objects_dir.is_dir() {
            return Err(anyhow!(
                "fatal: not a mini-git repository (or any of the parent directories): .mini-git"
            ));
        }

        self.write_object(None)
    }

    fn get_object_path(&self, hash_str: &str) -> Result<PathBuf> {
        let objects_dir = &self.objects_dir;

        if hash_str.len() != 40 {
            return Err(anyhow!("Invalid hash length: '{}'", hash_str));
        }

        let (dir_prefix, file_suffix) = hash_str.split_at(2);
        Ok(objects_dir.join(dir_prefix).join(file_suffix))
    }
}

#[derive(Encode, Decode, Debug, Ord, PartialOrd, Eq, PartialEq)]
// TODO(fcasibu): ctime, mtime
struct IndexEntry {
    mode: u32,
    sha1: [u8; 20],
    path: PathBuf,
}

#[derive(Encode, Decode, Debug, Ord, PartialOrd, Eq, PartialEq)]
struct IndexFile {
    entries: Vec<IndexEntry>,
}

#[derive(Parser, Debug)]
#[command(name = "mini-git", version, about = "A simplified Git clone")]
struct Cli {
    #[command(subcommand)]
    command: Commands,
}

#[derive(Subcommand, Debug)]
enum Commands {
    /// Initialize a new MiniGit repository
    Init,

    /// Compute object ID and optionally creates a blob from a file
    HashObject {
        file_path: Option<String>,

        /// Write object to object database
        #[arg(short)]
        write: bool,
    },

    /// Provide content or type information for repository objects
    CatFile {
        object_hash_input: Option<String>,

        /// Show type of given object
        #[arg(short = 't', conflicts_with = "print_content")]
        show_type: bool,

        /// Pretty-print given object content
        #[arg(short = 'p', conflicts_with = "show_type")]
        print_content: bool,
    },

    /// Register file contents in the workking directory to the index
    UpdateIndex {
        /// Add files not already in index
        #[arg(long)]
        add: String,
    },

    /// Information about files in index/working directory
    LsFiles {
        /// Show stage files in output
        #[arg(long)]
        stage: bool,
    },

    /// Create tree from the current inde
    WriteTree,
}

fn hash_content(content_with_header: &[u8]) -> [u8; 20] {
    let mut hasher = Sha1::new();
    hasher.update(content_with_header);
    hasher.finalize().into()
}

fn compress_content(content: &[u8]) -> Result<Vec<u8>> {
    let mut encoder = ZlibEncoder::new(Vec::new(), Compression::default());
    encoder.write_all(content)?;
    encoder.finish().map_err(anyhow::Error::from)
}

fn decompress_content(encoded_data: &[u8]) -> Result<Vec<u8>> {
    let mut decoder = ZlibDecoder::new(Vec::new());
    decoder.write_all(encoded_data)?;
    let decompressed_bytes = decoder.finish()?;
    Ok(decompressed_bytes)
}

fn handle_hash_object_command(
    file_path: Option<String>,
    write: bool,
    repository: &Repository,
) -> Result<()> {
    let mut input_data = String::new();

    if let Some(path) = file_path {
        let file_path = Path::new(&path);

        if !file_path.exists() {
            return Err(anyhow!("fatal: file does not exist {}", path));
        }

        input_data = fs::read_to_string(file_path)?;
    } else {
        io::stdin()
            .read_to_string(&mut input_data)
            .context("Failed to read from stdin")?;
        input_data = input_data.trim_end_matches('\n').to_string();
    }

    let mut encoded_hash = String::new();

    if write {
        let (_, hash_str) = repository.write_object(Some(&input_data))?;
        encoded_hash = hash_str;
    }

    println!("{}", encoded_hash);
    Ok(())
}

fn handle_cat_file_command(
    object_hash_input: Option<String>,
    show_type: bool,
    print_content: bool,
    repository: &Repository,
) -> Result<()> {
    let object_hash_str = match object_hash_input {
        Some(text) => text.trim().to_string(),
        None => {
            let mut buffer = String::new();
            io::stdin()
                .read_to_string(&mut buffer)
                .context("Failed to read object hash from stdin")?;
            buffer.trim().to_string()
        }
    };

    if object_hash_str.is_empty() {
        return Err(anyhow!("fatal: object hash cannot be empty"));
    }

    if object_hash_str.len() != 40 || !object_hash_str.chars().all(|c| c.is_ascii_hexdigit()) {
        return Err(anyhow!(
            "fatal: Not a valid object name: {}",
            object_hash_str
        ));
    }

    if !show_type && !print_content {
        return Err(anyhow!(
            "Error: you must specify one of -t (type) or -p (print)"
        ));
    }

    let object = repository.read_object(&object_hash_str)?;

    match object {
        GitObjects::Blob(blob_object) => {
            if show_type {
                println!("blob")
            }

            if print_content {
                println!("{}", &blob_object.raw_content);
            }
        }

        GitObjects::Tree(tree_object) => {
            if show_type {
                println!("tree")
            }

            if print_content {
                let raw = &tree_object.raw_content;
                let mut i = 0;

                while i < raw.len() {
                    let mode_start = i;
                    while raw[i] != b' ' {
                        i += 1;
                    }
                    let mode = std::str::from_utf8(&raw[mode_start..i])?;
                    i += 1;

                    let path_start = i;
                    while raw[i] != 0 {
                        i += 1;
                    }
                    let path = std::str::from_utf8(&raw[path_start..i])?;
                    i += 1;

                    if i + 20 > raw.len() {
                        return Err(anyhow::anyhow!("Malformed tree object: SHA-1 truncated"));
                    }
                    let sha1 = &raw[i..i + 20];
                    i += 20;

                    println!("{mode} {} {path}", hex::encode(sha1));
                }
            }
        }
    }

    Ok(())
}

fn handle_ls_files_command(stage: bool, repository: &Repository) -> Result<()> {
    if stage {
        let index_file = repository.read_index()?;

        for entry in index_file.entries {
            println!(
                "{} {} {}",
                entry.mode,
                encode(entry.sha1),
                entry.path.display()
            );
        }
    }

    Ok(())
}

fn handle_write_tree(repository: &Repository) -> Result<()> {
    let (_, hash_str) = repository.write_tree()?;
    println!("{hash_str}");

    Ok(())
}

fn main() -> Result<()> {
    let cli = Cli::parse();

    let repository = Repository::new()?;

    match cli.command {
        Commands::Init => {
            repository.init()?;
        }
        Commands::HashObject { file_path, write } => {
            handle_hash_object_command(file_path, write, &repository)?
        }
        Commands::CatFile {
            object_hash_input,
            show_type,
            print_content,
        } => handle_cat_file_command(object_hash_input, show_type, print_content, &repository)?,
        Commands::UpdateIndex { add } => {
            repository.add_to_index(&PathBuf::new().join(&add))?;
        }
        Commands::LsFiles { stage } => {
            handle_ls_files_command(stage, &repository)?;
        }
        Commands::WriteTree => handle_write_tree(&repository)?,
    }

    Ok(())
}
