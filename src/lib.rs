use pgrx::prelude::*;
use std::{fs, path::PathBuf};

pgrx::pg_module_magic!();

// pgfs_copy_dir will copy a source directory to a destination directory
// and return true if successful, false if it fails.
// We need to preserve the permissions of the source directory as well
#[pg_extern]
fn pgfs_copy_dir(source_dir: &str, dest_dir: &str) -> bool {
    let source_path = PathBuf::from(source_dir);
    let dest_path = PathBuf::from(dest_dir);

    // Check if source directory exists
    if !source_path.exists() {
        pgrx::info!("Source directory {} does not exist", source_dir);
        return false;
    }

    // Check if destination directory exists if it doesn't then create it.
    if !dest_path.exists() {
        match fs::create_dir_all(&dest_path) {
            Ok(_) => pgrx::info!("Created destination directory: {}", dest_dir),
            Err(err) => {
                pgrx::info!("Failed to create destination directory: {:?}", err);
                return false;
            }
        }
    }

    // Iterate over the source directory and copy the files and folders to
    // the destination directory
    match fs::read_dir(source_path) {
        Ok(entries) => {
            for entry in entries.flatten() {
                let entry_path = entry.path();
                let entry_name = entry.file_name();
                let dest_path = dest_path.join(entry_name);

                if entry_path.is_dir() {
                    // Recursively copy the directory
                    if !pgfs_copy_dir(entry_path.to_str().unwrap(), dest_path.to_str().unwrap()) {
                        return false;
                    }
                } else {
                    // If it's a file just copy it
                    if fs::copy(&entry_path, dest_path).is_err() {
                        pgrx::info!("Failed to copy file: {:?}", entry_path);
                        return false;
                    }
                }
            }
        }
        Err(err) => {
            pgrx::info!("Failed to read source directory: {:?}", err);
            return false;
        }
    }
    pgrx::info!("Successfully copied directory");
    true
}

#[cfg(any(test, feature = "pg_test"))]
#[pg_schema]
// Test the pgfs_mkdir functioni to assert that the directory was created successfully
mod tests {
    use pgrx::*;
    use std::fs;

    #[pg_test]
    fn test_pgfs_copy_dir() {
        let source_dir = "/tmp/test_dir";
        let dest_dir = "/tmp/test_dir_copy";

        // Create the source directory
        let _ = fs::create_dir_all(source_dir);

        // Create a file in the source directory
        let file_path = format!("{}/test_file", source_dir);
        let _ = fs::File::create(file_path);

        // Copy the source directory to the destination directory
        let result = match Spi::get_one::<bool>(
            format!("SELECT pgfs_copy_dir('{}', '{}')", source_dir, dest_dir).as_str(),
        ) {
            Ok(result) => result,
            Err(err) => {
                pgrx::error!("Failed to copy directory: {:?}", err);
            }
        };
        assert_eq!(result, Some(true));

        // Clean up the directories
        let _ = fs::remove_dir_all(source_dir);
        let _ = fs::remove_dir_all(dest_dir);
    }
}

#[cfg(test)]
pub mod pg_test {
    // pg_test module with both the setup and postgresql_conf_options functions are required

    use std::vec;

    pub fn setup(_options: Vec<&str>) {}

    pub fn postgresql_conf_options() -> Vec<&'static str> {
        // return any postgresql.conf settings that are required for your tests
        // uncomment this when there are tests for the partman background worker
        // vec!["shared_preload_libraries = 'pg_partman_bgw'"]
        vec![]
    }
}
