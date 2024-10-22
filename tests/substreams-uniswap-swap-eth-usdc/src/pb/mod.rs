// @generated
pub mod contract {
    // @@protoc_insertion_point(attribute:contract.v1)
    pub mod v1 {
        include!("contract.v1.rs");
        // @@protoc_insertion_point(contract.v1)
    }
}
// @@protoc_insertion_point(attribute:parquet)
pub mod parquet {
    include!("parquet.rs");
    // @@protoc_insertion_point(parquet)
}
pub mod sf {
    pub mod ethereum {
        pub mod r#type {
            // @@protoc_insertion_point(attribute:sf.ethereum.type.v2)
            pub mod v2 {
                include!("sf.ethereum.type.v2.rs");
                // @@protoc_insertion_point(sf.ethereum.type.v2)
            }
        }
    }
    // @@protoc_insertion_point(attribute:sf.substreams)
    pub mod substreams {
        include!("sf.substreams.rs");
        // @@protoc_insertion_point(sf.substreams)
        pub mod ethereum {
            // @@protoc_insertion_point(attribute:sf.substreams.ethereum.v1)
            pub mod v1 {
                include!("sf.substreams.ethereum.v1.rs");
                // @@protoc_insertion_point(sf.substreams.ethereum.v1)
            }
        }
        // @@protoc_insertion_point(attribute:sf.substreams.v1)
        pub mod v1 {
            include!("sf.substreams.v1.rs");
            // @@protoc_insertion_point(sf.substreams.v1)
        }
    }
}
