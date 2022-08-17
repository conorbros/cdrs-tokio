use crate::common::get_struct_fields;
use proc_macro2::TokenStream;
use quote::*;
use syn::DeriveInput;

pub fn impl_try_from_row(ast: &DeriveInput) -> TokenStream {
    let name = &ast.ident;
    let fields = get_struct_fields(ast);

    quote! {
        impl cdrs_tokio::envelope::TryFromRow for #name {
            fn try_from_row(cdrs: cdrs_tokio::types::rows::Row) -> cdrs_tokio::Result<Self> {
                #[allow(unused_imports)]
                use cdrs_tokio::envelope::TryFromUdt;
                #[allow(unused_imports)]
                use cdrs_tokio::types::from_cdrs::FromCdrsByName;
                #[allow(unused_imports)]
                use cdrs_tokio::types::IntoRustByName;
                #[allow(unused_imports)]
                use cdrs_tokio::types::AsRustType;

                Ok(#name {
                  #(#fields),*
                })
            }
        }
    }
}
