use crate::mapping::errors::MappingError;
use thiserror::Error;
use crate::templates::errors::TemplateError;

#[derive(Error, Debug)]
pub enum MaplibError {
    #[error(transparent)]
    TemplateError(#[from] TemplateError),
    #[error(transparent)]
    MappingError(#[from] MappingError),
}