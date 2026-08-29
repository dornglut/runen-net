//! Standalone RunenNet semantic core.
//!
//! The normative RunenNet specification lives in the repository `spec/` tree.
//! Rust representations in this crate are implementation choices unless a
//! specification revision explicitly standardizes them.

pub mod delivery;
mod delivery_acceptance;
pub use delivery_acceptance::DeliveryAcceptance;
pub mod identity;
pub mod input;
pub mod protocol;
mod protocol_declaration;
pub use protocol_declaration::{
    CompatibilityOfferBuilder, SchemaContractOfferBuilder, SchemaOfferBuilder,
};
pub mod replication;
pub mod session;
