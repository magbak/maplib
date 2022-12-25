use std::fmt::{Display, Formatter};
use oxrdf::IriParseError;
use thiserror::Error;
use crate::errors::TriplestoreError;

#[derive(Error, Debug)]
pub enum ShaclError {
    TriplestoreError(TriplestoreError),
    IriParseError(IriParseError),
    ListMissingFirstElementError(String),
    ListMissingRestError(String)
}

impl Display for ShaclError {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            ShaclError::TriplestoreError(e) => {
                write!(f, "Triplestore error during SHACL processing: {}", e)
            }
            ShaclError::IriParseError(e) => {
                write!(f, "IriParseError during SHACL processing: {}", e)
            }
            ShaclError::ListMissingFirstElementError(s) => {
                write!(f, "List is missing first element at {}", s)
            }
            ShaclError::ListMissingRestError(s) => {
                write!(f, "List is missing rest error at {}", s)
            }
        }
    }
}