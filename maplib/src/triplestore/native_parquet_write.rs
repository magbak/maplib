use super::Triplestore;
use std::path::Path;
use std::time::Instant;
use log::debug;
use rayon::iter::ParallelDrainRange;
use rayon::iter::ParallelIterator;
use crate::mapping::errors::MappingError;
use crate::mapping::RDFNodeType;
use crate::triplestore::parquet::{property_to_filename, write_parquet};

impl Triplestore {
    pub fn write_native_parquet(&mut self, path: &Path) -> Result<(), MappingError>{
        let now = Instant::now();
        if !path.exists() {
            return Err(MappingError::PathDoesNotExist(path.to_str().unwrap().to_string()))
        }
        let path_buf = path.to_path_buf();

        self.deduplicate()?;

        let mut dfs_to_write = vec![];

        for (property, tts) in &mut self.df_map {
            for (rdf_node_type, tt) in tts {
                let filename;
                if let RDFNodeType::Literal(literal_type) = rdf_node_type {
                    filename = format!(
                        "{}_{}",
                        property_to_filename(property),
                        property_to_filename(literal_type.as_str())
                    );
                } else {
                    filename = format!(
                        "{}_object_property",
                        property_to_filename(property),
                    )
                }
                let file_path = path_buf.clone();
                if let Some(_) = &self.caching_folder{ } else {
                    for (i, df) in tt.dfs.as_mut().unwrap().iter_mut().enumerate() {
                        let filename = format!("{filename}_part_{i}.parquet");
                        let mut file_path = file_path.clone();
                        file_path.push(filename);
                        dfs_to_write.push((df, file_path));
                    }
                }

            }
        }

        let results:Vec<Result<(), MappingError>> = dfs_to_write.par_drain(..).map(|(df, file_path)|write_parquet(df, file_path.as_path())).collect();
        for r in results {
            r?;
        }

        debug!("Writing native parquet took {} seconds", now.elapsed().as_secs_f64());
        Ok(())
    }
}
