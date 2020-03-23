extern crate proc_macro;

use crate::proc_macro::TokenStream;
use quote::quote;
use syn;

#[proc_macro_derive(EventType)]
pub fn event_type_derive(input: TokenStream) -> TokenStream {
    let ast = syn::parse(input).unwrap();

    impl_event_type(&ast)
}

fn impl_event_type(ast: &syn::DeriveInput) -> TokenStream {
    let name = &ast.ident;
    let gen = quote! {
        impl EventType for #name {
            fn code() -> String {
                stringify!(#name).to_owned()
            }
        }
    };
    gen.into()
}

#[proc_macro_derive(ConsumerGroup)]
pub fn consumer_group_derive(input: TokenStream) -> TokenStream {
    let ast = syn::parse(input).unwrap();

    impl_consumer_group(&ast)
}

fn impl_consumer_group(ast: &syn::DeriveInput) -> TokenStream {
    let name = &ast.ident;
    let gen = quote! {
        impl ConsumerGroup for #name {
            fn group() -> String {
                stringify!(#name).to_owned()
            }
        }
    };
    gen.into()
}
