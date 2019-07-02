#[macro_use]
mod macros;

type Stuff = String;

#[test]
fn test() {
    jsonrpc_client!(pub struct RpcClient {
        single:
            pub fn a(&self, i: usize) -> Result<u64>;
        enum:
            pub fn b(&self) -> Result<A(Stuff)|B(Vec<String>)>;
    });
}
