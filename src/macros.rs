#[macro_export]
macro_rules! jsonrpc_client {
    (
        $(#[$struct_attr:meta])*
        pub struct $struct_name:ident {
            $(
                single:
                $(
                    $(#[$attr_a:meta])*
                    pub fn $method_a:ident(&self$(, $arg_name_a:ident: $arg_ty_a:ty)*) -> Result<$return_ty_a:ty>;
                )*
                enum:
                $(
                    $(#[$attr_b:meta])*
                    pub fn $method_b:ident(&self$(, $arg_name_b:ident: $arg_ty_b:ty)*) -> Result<$($title:ident($return_ty_b:ty))|*>;
                )*
            )+
        }
    ) => {
        use failure::Error;
        use reqwest as rq;
        use serde::Deserialize;
        use serde::Serialize;
        use std::marker::PhantomData;
        use std::sync::{Arc, Condvar, Mutex};

        #[derive(Deserialize)]
        struct RpcResponse<T> {
            pub result: Option<T>,
            pub error: Option<serde_json::Value>,
            pub id: Option<usize>,
        }

        #[derive(Serialize)]
        struct RpcRequestSer<'a, T> {
            pub method: &'static str,
            pub params: &'a T,
            pub id: usize,
        }

        #[derive(Serialize)]
        struct RpcRequest<T> {
            pub method: &'static str,
            pub params: T,
        }

        impl<T> RpcRequest<T>
        where T: Serialize {
            pub fn polymorphize(self) -> RpcRequest<serde_json::Value> {
                RpcRequest {
                    method: self.method,
                    params: serde_json::to_value(self.params).unwrap(),
                }
            }

            pub fn as_ser(&self, id: usize) -> RpcRequestSer<T> {
                RpcRequestSer {
                    method: self.method,
                    params: &self.params,
                    id,
                }
            }
        }

        pub mod reply {
            use failure::Error;
            use super::*;
            $(
                $(
                    $(#[$attr_b])*
                    #[derive(Debug)]
                    #[allow(non_camel_case_types)]
                    pub enum $method_b {
                        $($title($return_ty_b),)+
                    }

                    $(#[$attr_b])*
                    impl $method_b {
                        $(
                            #[allow(non_snake_case)]
                            pub fn $title(self) -> Result<$return_ty_b, Error> {
                                match self {
                                    $method_b::$title(a) => Ok(a),
                                    a => failure::bail!("Wrong variant of {}: expected {}(_), got {:?}", stringify!(reply::$method_b), stringify!(reply::$method_b::$return_ty_b), a)
                                }
                            }
                        )+
                    }
               )*
            )+
        }

        pub struct ReqBatcher<T, U: for<'de> Deserialize<'de>> {
            reqs: Vec<RpcRequest<serde_json::Value>>,
            resps: Vec<Option<U>>,
            max_batch_size: usize,
            phantom: PhantomData<T>,
        }

        pub struct BatcherPair<'a, T: for<'de> Deserialize<'de>>(&'a $struct_name, ReqBatcher<$struct_name, T>);
        impl<'a, T> BatcherPair<'a, T>
        where
            T: for<'de> Deserialize<'de>
        {
            fn add_req<U: Serialize>(&mut self, method: &'static str, params: U) -> Result<usize, Error> {
                let body = RpcRequest {
                    method,
                    params,
                }.polymorphize();
                if self.inner().max_batch_size > 0 && self.inner().reqs.len() >= self.inner().max_batch_size {
                    self.flush()?;
                }
                let id = self.inner().reqs.len();
                self.inner().reqs.push(body);
                Ok(id)
            }
        }

        pub trait BatchRequest<$struct_name, T: for<'de> Deserialize<'de>> {
            fn inner(&mut self) -> &mut ReqBatcher<$struct_name, T>;
            $(
                $(
                    $(#[$attr_a])*
                    fn $method_a(&mut self$(, $arg_name_a: $arg_ty_a)*) -> Result<usize, Error>;
                )*
                $(
                    $(#[$attr_b])*
                    fn $method_b(&mut self$(, $arg_name_b: $arg_ty_b)*) -> Result<usize, Error>;
                )*
            )*
            fn flush(&mut self) -> Result<(), Error>;
            fn send(&mut self) -> Result<Vec<T>, Error>;
        }

        impl<'a, T> BatchRequest<$struct_name, T> for BatcherPair<'a, T>
        where T: for<'de> Deserialize<'de> {
            fn inner(&mut self) -> &mut ReqBatcher<$struct_name, T> {
                &mut self.1
            }

            $(
                $(
                    $(#[$attr_a])*
                    fn $method_a(&mut self$(, $arg_name_a: $arg_ty_a)*) -> Result<usize, Error> {
                        self.add_req(stringify!($method_a), ($($arg_name_a,)*))
                    }
                )*
                $(
                    $(#[$attr_b])*
                    fn $method_b(&mut self$(, $arg_name_b: $arg_ty_b)*) -> Result<usize, Error> {
                        self.add_req(stringify!($method_b), ($($arg_name_b,)*))
                    }
                )*
            )*

            fn flush(&mut self) -> Result<(), Error> {
                if self.inner().reqs.len() == 0 {
                    return Ok(())
                }
                let mut res = self.0.dispatch(&self.inner().reqs.iter().enumerate().map(|(idx, a)| a.as_ser(idx)).collect::<Vec<_>>())?;
                let text = res.text()?;
                let json = match serde_json::from_str::<Vec<RpcResponse<T>>>(&text) {
                    Ok(a) => a,
                    Err(_) => {
                        failure::bail!("{:?}", serde_json::from_str::<RpcResponse<serde_json::Value>>(&text)?.error)
                    }
                };
                let res_res: Result<Vec<(usize, T)>, Error> = json.into_iter().map(|reply| {
                    Ok(match reply.result {
                        Some(b) => (reply.id.ok_or(failure::format_err!("missing id in response"))?, b),
                        _ => failure::bail!("{:?}", reply.error),
                    })
                }).collect();
                let res = res_res?;
                let req_count = self.inner().reqs.len();
                let resps_count = self.inner().resps.len();
                self.inner().resps.extend(std::iter::repeat_with(|| None).take(req_count));
                for r in res {
                    self.inner().resps[resps_count + r.0] = Some(r.1);
                }
                self.inner().reqs = Vec::new();
                Ok(())
            }

            fn send(&mut self) -> Result<Vec<T>, Error> {
                self.flush()?;
                let res = std::mem::replace(&mut self.inner().resps, Vec::new());
                let res = res.into_iter()
                    .map(|r| r.ok_or(failure::format_err!("missing response")))
                    .collect::<Result<Vec<T>, Error>>()?;
                Ok(res)
            }
        }

        $(#[$struct_attr])*
        pub struct $struct_name {
            uri: String,
            user: Option<String>,
            pass: Option<String>,
            max_concurrency: usize,
            rps: usize,
            counter: (Mutex<usize>, Condvar),
            last_req: Mutex<std::time::Instant>,
            max_batch_size: usize,
            client: rq::Client,
        }

        impl $struct_name {
            pub fn new(uri: String, user: Option<String>, pass: Option<String>, max_concurrency: usize, rps: usize, max_batch_size: usize) -> Arc<Self> {
                Arc::new($struct_name {
                    uri,
                    user,
                    pass,
                    max_concurrency,
                    rps,
                    counter: (Mutex::new(0), Condvar::new()),
                    last_req: Mutex::new(std::time::Instant::now()),
                    max_batch_size,
                    client: rq::Client::new(),
                })
            }

            pub fn batcher<'a, T: for<'de> Deserialize<'de>>(&'a self) -> BatcherPair<'a, T> {
                BatcherPair(self, ReqBatcher {
                    reqs: Vec::new(),
                    resps: Vec::new(),
                    max_batch_size: self.max_batch_size,
                    phantom: PhantomData,
                })
            }

            fn call_method<T: Serialize>(&self, method: &'static str, params: T) -> Result<String, Error> {
                let mut res = self.dispatch(&RpcRequest {
                    method,
                    params,
                })?;
                let txt = res.text()?;
                Ok(txt)
            }

            fn dispatch<T: Serialize>(&self, data: &T) -> Result<rq::Response, Error> {
                let mut builder = self.client
                    .post(&self.uri);
                match (&self.user, &self.pass) {
                    (Some(ref u), Some(ref p)) => builder = builder.basic_auth(u, Some(p)),
                    (Some(ref u), None) => builder = builder.basic_auth::<&str, &str>(u, None),
                    _ => (),
                };
                builder = builder.json(data);
                if self.rps > 0 {
                    let wait = std::time::Duration::from_secs(1) / self.rps as u32;
                    let mut lock = self.last_req.lock().unwrap();
                    let elapsed = lock.elapsed();
                    if elapsed < wait {
                        std::thread::sleep(wait - elapsed);
                    }
                    *lock = std::time::Instant::now();
                    drop(lock);
                }
                if self.max_concurrency > 0 {
                    let mut lock = self.counter.0.lock().unwrap();
                    while *lock == self.max_concurrency {
                        lock = self.counter.1.wait(lock).unwrap();
                    }
                    *lock = *lock + 1;
                    drop(lock);
                }
                let res = builder.send();
                if self.max_concurrency > 0 {
                    let mut lock = self.counter.0.lock().unwrap();
                    *lock = *lock - 1;
                    drop(lock);
                    self.counter.1.notify_one();
                }
                let res = res?;
                Ok(res)
            }

            $(
                $(
                    $(#[$attr_a])*
                    pub fn $method_a(&self$(, $arg_name_a: $arg_ty_a)*) -> Result<$return_ty_a, Error> {
                        let txt = self.call_method(stringify!($method_a), ($($arg_name_a,)*))?;
                        let body: RpcResponse<$return_ty_a> = serde_json::from_str(&txt)?;
                        match body.error {
                            Some(e) => failure::bail!("{:?}", e),
                            None => body.result.ok_or(failure::format_err!("null response")),
                        }
                    }
                )*
                $(
                    $(#[$attr_b])*
                    pub fn $method_b(&self$(, $arg_name_b: $arg_ty_b)*) -> Result<reply::$method_b, Error> {
                        let txt = self.call_method(stringify!($method_b), ($($arg_name_b,)*))?;
                        let body: reply::$method_b = (|txt: String| {
                            $(
                                match serde_json::from_str::<RpcResponse<$return_ty_b>>(&txt) {
                                    Ok(a) => match a.error {
                                        Some(e) => failure::bail!("{:?}", e),
                                        None => return Ok(reply::$method_b::$title(a.result.ok_or(failure::format_err!("null response"))?)),
                                    },
                                    Err(_) => (),
                                };
                            )+
                            Err(failure::format_err!("Cannot deserialize to any variant of reply::{}", stringify!($method_b)))
                        })(txt)?;
                        Ok(body)
                    }
                )*
            )*
        }
    };
}
