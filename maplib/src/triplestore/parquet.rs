use crate::mapping::errors::MappingError;
use nom::InputIter;
use polars::prelude::{LazyFrame, ParallelStrategy, ParquetWriter, ScanArgsParquet};
use polars_core::frame::DataFrame;
use std::cmp::min;
use std::fs::File;
use std::path::{Path, PathBuf};
use uuid::Uuid;

const PARQUET_DF_SIZE: usize = 50_000_000;

pub(crate) fn property_to_filename(property_name: &str) -> String {
    property_name
        .iter_elements()
        .filter(|x| x.is_alphanumeric())
        .collect()
}

pub(crate) fn write_parquet(df: &mut DataFrame, file_path: &Path) -> Result<(), MappingError> {
    let file = File::create(file_path).map_err(|x| MappingError::FileCreateIOError(x))?;
    let mut writer = ParquetWriter::new(file);
    writer = writer.with_row_group_size(Some(1_000));
    writer
        .finish(df)
        .map_err(|x| MappingError::WriteParquetError(x))?;
    Ok(())
}

pub(crate) fn read_parquet(file_path: &String) -> Result<LazyFrame, MappingError> {
    LazyFrame::scan_parquet(
        Path::new(file_path),
        ScanArgsParquet {
            n_rows: None,
            cache: false,
            parallel: ParallelStrategy::Auto,
            rechunk: true,
            row_count: None,
            low_memory: false,
        },
    )
    .map_err(|x| MappingError::ReadParquetError(x))
}

pub(crate) fn split_write_tmp_df(
        caching_folder: &str,
        df: DataFrame,
        predicate: &str,
    ) -> Result<Vec<String>, MappingError> {
        let n_of_size = (df.estimated_size() / PARQUET_DF_SIZE) + 1;
        let chunk_size = df.height() / n_of_size;
        let mut offset = 0i64;
        let mut paths = vec![];
        loop {
            let to_row = min(df.height(), offset as usize + chunk_size);
            let mut df_slice = df.slice_par(offset, to_row);
            let file_name = format!("tmp_{}_{}.parquet", predicate, Uuid::new_v4().to_string());
            let path_buf: PathBuf = [caching_folder, &file_name].iter().collect();
            let path = path_buf.as_path();
            write_parquet(&mut df_slice, path)?;
            paths.push(path.to_str().unwrap().to_string());
            offset += chunk_size as i64;
            if offset >= df.height() as i64 {
                break;
            }
        }
        Ok(paths)
    }