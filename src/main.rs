use anyhow::{Context, Result, anyhow};
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
}

impl BlobObject {
    fn new(raw_content: &str) -> Result<Self> {
        let object_content = format!("blob {}\0{}", raw_content.bytes().len(), raw_content);
        let compressed_content = compress_content(&object_content)
            .with_context(|| format!("Failed to compress blob content"))?;
        Ok(BlobObject {
            hash: hash_content(&object_content),
            compressed_content,
        })
    }
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

fn handle_init_command() -> Result<()> {
    let mini_git_dir = env::current_dir()?.join(".mini-git");

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

    fs::create_dir_all(mini_git_dir.join("objects"))
        .context("Failed to create objects directory")?;
    fs::create_dir_all(mini_git_dir.join("refs").join("heads"))
        .context("Failed to create refs/heads directory")?;
    fs::create_dir_all(mini_git_dir.join("refs").join("tags"))
        .context("Failed to create refs/tags directory")?;

    let head_file = mini_git_dir.join("HEAD");
    if !head_file.exists() {
        fs::write(&head_file, "ref: refs/heads/main\n")
            .with_context(|| format!("Failed to write HEAD file at {}", head_file.display()))?;
    }

    Ok(())
}

fn get_object_path(base_objects_dir: &Path, hash_str: &str) -> Result<PathBuf> {
    if hash_str.len() != 40 {
        return Err(anyhow!("Invalid hash length: '{}'", hash_str));
    }
    let (dir_prefix, file_suffix) = hash_str.split_at(2);
    Ok(base_objects_dir.join(dir_prefix).join(file_suffix))
}

fn handle_hash_object_command(file_path: Option<String>, write: bool) -> Result<()> {
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

    let blob = BlobObject::new(&input_data)?;
    let encoded_hash = encode(blob.hash);

    if write {
        let current_dir = env::current_dir()?;
        let objects_dir = current_dir.join(".mini-git/objects");

        if !objects_dir.is_dir() {
            return Err(anyhow!(
                "fatal: not a mini-git repository (or any of the parent directories): .mini-git"
            ));
        }

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
    }

    println!("{}", encoded_hash);
    Ok(())
}

fn handle_cat_file_command(
    object_hash_input: Option<String>,
    show_type: bool,
    print_content: bool,
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

    let current_dir = env::current_dir()?;
    let objects_dir = current_dir.join(".mini-git/objects");

    if !objects_dir.is_dir() {
        return Err(anyhow!(
            "fatal: not a mini-git repository (or any of the parent directories): .mini-git"
        ));
    }

    let object_file_path = get_object_path(&objects_dir, &object_hash_str)?;

    if !object_file_path.exists() {
        return Err(anyhow!(
            "fatal: Not a valid object name {}",
            object_hash_str
        ));
    }

    let compressed_data = fs::read(&object_file_path)
        .with_context(|| format!("Failed to read object file {}", object_file_path.display()))?;

    let decompressed_object_data = decompress_content(&compressed_data)
        .with_context(|| format!("Failed to decompress object {}", object_hash_str))?;

    if show_type {
        if let Some(space_pos) = decompressed_object_data.find(' ') {
            println!("{}", &decompressed_object_data[..space_pos]);
        } else {
            return Err(anyhow!(
                "fatal: malformed object {} (missing type or space)",
                object_hash_str
            ));
        }
    }

    if print_content {
        if let Some(null_pos) = decompressed_object_data.find('\0') {
            println!("{}", &decompressed_object_data[null_pos + 1..]);
        } else {
            return Err(anyhow!(
                "fatal: malformed object {} (missing null terminator)",
                object_hash_str
            ));
        }
    }

    Ok(())
}

fn main() -> Result<()> {
    let cli = Cli::parse();

    match cli.command {
        Commands::Init => handle_init_command()?,
        Commands::HashObject { file_path, write } => handle_hash_object_command(file_path, write)?,
        Commands::CatFile {
            object_hash_input,
            show_type,
            print_content,
        } => handle_cat_file_command(object_hash_input, show_type, print_content)?,
    }

    Ok(())
}
