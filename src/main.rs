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
    fn new(raw_content: &str) -> Result<Self> {
        let object_content = format!("blob {}\0{}", raw_content.bytes().len(), raw_content);
        let compressed_content = compress_content(&object_content)
            .with_context(|| format!("Failed to compress blob content"))?;

        Ok(BlobObject {
            hash: hash_content(&object_content),
            compressed_content,
            raw_content: raw_content.to_string(),
        })
    }
}

enum GitObjects {
    Blob(BlobObject),
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

    pub fn write_object(&self, input_data: &str) -> Result<[u8; 20]> {
        let objects_dir = &self.objects_dir;

        if !objects_dir.is_dir() {
            return Err(anyhow!(
                "fatal: not a mini-git repository (or any of the parent directories): .mini-git"
            ));
        }

        let blob = BlobObject::new(&input_data)?;
        let encoded_hash = encode(blob.hash);

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

        fs::write(&object_file_path, &blob.compressed_content).with_context(|| {
            format!("Failed to write object file {}", object_file_path.display())
        })?;

        Ok(blob.hash)
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

        let encoded_hash = self.write_object(&data)?;

        let mut index = self.read_index()?;

        if let Some(pos) = index.entries.iter().position(|e| e.path == file_path_buf) {
            if index.entries[pos].sha1 != encoded_hash {
                index.entries[pos] = IndexEntry {
                    mode: 100644,
                    sha1: encoded_hash,
                    path: file_path_buf,
                };
            }
        } else {
            index.entries.push(IndexEntry {
                mode: 100644,
                sha1: encoded_hash,
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

        let decompressed_data = decompress_content(&compressed_data)
            .with_context(|| format!("Failed to decompress object {}", object_hash_str))?;

        let position = decompressed_data
            .find(' ')
            .with_context(|| format!("Invalid content {}", object_hash_str))?;

        let object_type = &decompressed_data[0..position];
        let null_terminator_position = decompressed_data
            .find('\0')
            .with_context(|| format!("Invalid content {}", object_hash_str))?;

        match object_type {
            "blob" => Ok(GitObjects::Blob(BlobObject::new(
                &decompressed_data[null_terminator_position..],
            )?)),
            _ => Err(anyhow!(
                "Object type \"{}\" not yet implemented",
                object_type
            )),
        }
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
}

fn hash_content(content_with_header: &str) -> [u8; 20] {
    let mut hasher = Sha1::new();
    hasher.update(content_with_header.as_bytes());
    hasher.finalize().into()
}

fn compress_content(content: &str) -> Result<Vec<u8>> {
    let mut encoder = ZlibEncoder::new(Vec::new(), Compression::default());
    encoder.write_all(content.as_bytes())?;
    encoder.finish().map_err(anyhow::Error::from)
}

fn decompress_content(encoded_data: &[u8]) -> Result<String> {
    let mut decoder = ZlibDecoder::new(Vec::new());
    decoder.write_all(encoded_data)?;
    let decompressed_bytes = decoder.finish()?;
    String::from_utf8(decompressed_bytes).map_err(anyhow::Error::from)
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
        encoded_hash = String::from_utf8(repository.write_object(&input_data)?.to_vec())?;
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
    }

    Ok(())
}
