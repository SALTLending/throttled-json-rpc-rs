use anyhow::Result;
use futures::prelude::*;
use reqwest;
use std::sync::Arc;

/**
    There are times that we want to clean the trailing nulls, because then it works better for some implementations
    of Nodes where it figures out the optionals by the count of the params via the json-rpc.
*/
fn values_cleanse(values: Vec<serde_json::Value>) -> Vec<serde_json::Value> {
    use serde_json::Value::Null;
    values
        .into_iter()
        .rev()
        .skip_while(|some_value| match some_value {
            Null => true,
            _ => false,
        })
        .collect::<Vec<serde_json::Value>>()
        .into_iter()
        .rev()
        .collect::<Vec<serde_json::Value>>()
}

pub struct ReqBatcher {
    ask_request: futures::channel::mpsc::Sender<RequestCallbackPair>,
    client_options: ClientOptions,
}

#[derive(Debug, Clone)]
pub struct ClientOptions {
    uri: String,
    rps: RPS,
    client_auth: Option<ClientAuth>,
}

#[derive(Debug, Clone)]
pub struct ClientAuth {
    pub user: String,
    pub password: Option<String>,
}

#[derive(Debug, Clone, Copy)]
enum RpsState {
    None,
    Limit(f64),
}

type RequestCallbackPair = (
    Vec<serde_json::Value>,
    futures::channel::oneshot::Sender<Result<serde_json::Value>>,
);

#[derive(Debug, Clone)]
pub struct RPS(RpsState);
impl RPS {
    pub fn new(value: f64) -> Result<Self> {
        if value <= 0.0 {
            return Err(anyhow::anyhow!("Invalid RPS value: {} <= 0", value));
        }
        Ok(RPS(RpsState::Limit(value)))
    }
    pub fn none() -> Self {
        RPS(RpsState::None)
    }
}

impl ReqBatcher {
    pub fn new(client_options: ClientOptions) -> Arc<Self> {
        let (ask_request, requests) = futures::channel::mpsc::channel(10_000);
        let req_batcher = Arc::new(ReqBatcher {
            ask_request,
            client_options,
        });
        let answer = req_batcher.clone();
        tokio::spawn(async move {
            requests
                .map(|x| (req_batcher.clone(), x))
                .for_each_concurrent(10, |(req_batcher, x)| async move {
                    req_batcher.request(x).await
                })
                .await;
        });
        answer
    }

    async fn request(&self, (data, respond): RequestCallbackPair) {
        let client = reqwest::Client::new();
        let client_options = &self.client_options;
        let mut builder = client.post(&client_options.uri);
        if let Some(client_auth) = &client_options.client_auth {
            builder = builder.basic_auth(client_auth.user.clone(), client_auth.password.clone());
        }
        builder = builder.json(&data);
        let res = builder.send();
        if let RPS(RpsState::Limit(rps)) = client_options.rps {
            let wait = if rps > 1.0 {
                std::time::Duration::from_secs(1) / rps as u32
            } else {
                std::time::Duration::from_secs(1) * (1.0 / rps) as u32
            };
            tokio::time::delay_for(wait).await;
        }
        let response: Result<serde_json::Value> = (|| async move {
            let res = res.await?;
            Ok(res.json().await?)
        })()
        .await;
        respond.send(response).unwrap_or_else(|e| {
            log::error!(
                "Had an error responding to an one time with values = ({:?}) with error ({:?})",
                data,
                e
            )
        });
    }
}

// pub mod reply {
//     use anyhow::Error;
//     $(
//         $(
//             $(#[$attr_b])*
//             #[derive(Debug)]
//             #[allow(non_camel_case_types)]
//             pub enum $method_b {
//                 $($title($return_ty_b),)+
//             }

//             $(#[$attr_b])*
//             impl $method_b {
//                 $(
//                     #[allow(non_snake_case)]
//                     pub fn $title(self) -> Result<$return_ty_b, Error> {
//                         match self {
//                             $method_b::$title(a) => Ok(a),
//                             a => anyhow::bail!("Wrong variant of {}: expected {}(_), got {:?}", stringify!(reply::$method_b), stringify!(reply::$method_b::$return_ty_b), a)
//                         }
//                     }
//                 )+
//             }
//        )*
//     )+
// }

// impl<'a, T> BatcherPair<'a, T>
// where
//     T: for<'de> Deserialize<'de>
// {
//     fn add_req<U: Serialize>(&mut self, method: &'static str, params: U) -> Result<usize, Error> {
//         let body = RpcRequest {
//             method,
//             params,
//         }.polymorphize();
//         if self.inner().max_batch_size > 0 && self.inner().reqs.len() >= self.inner().max_batch_size {
//             self.flush()?;
//         }
//         let id = self.inner().reqs.len();
//         self.inner().reqs.push(body);
//         Ok(id)
//     }
// }

// pub trait BatchRequest<$struct_name, T: for<'de> Deserialize<'de>> {
//     fn inner(&mut self) -> &mut ReqBatcher<$struct_name, T>;
//     $(
//         $(
//             $(#[$attr_a])*
//             fn $method_a(&mut self$(, $arg_name_a: $arg_ty_a)*) -> Result<usize, Error>;
//         )*
//         $(
//             $(#[$attr_b])*
//             fn $method_b(&mut self$(, $arg_name_b: $arg_ty_b)*) -> Result<usize, Error>;
//         )*
//     )*
//     fn flush(&mut self) -> Result<(), Error>;
//     fn send(&mut self) -> Result<Vec<T>, Error>;
// }

// impl<'a, T> BatchRequest<$struct_name, T> for BatcherPair<'a, T>
// where T: for<'de> Deserialize<'de> {
//     fn inner(&mut self) -> &mut ReqBatcher<$struct_name, T> {
//         &mut self.1
//     }

//     $(
//         $(
//             $(#[$attr_a])*
//             fn $method_a(&mut self$(, $arg_name_a: $arg_ty_a)*) -> Result<usize, Error> {
//                 self.add_req(stringify!($method_a), ($($arg_name_a,)*))
//             }
//         )*
//         $(
//             $(#[$attr_b])*
//             fn $method_b(&mut self$(, $arg_name_b: $arg_ty_b)*) -> Result<usize, Error> {
//                 self.add_req(stringify!($method_b), ($($arg_name_b,)*))
//             }
//         )*
//     )*

//     async fn flush(&mut self) -> Result<(), Error> {
//         if self.inner().reqs.len() == 0 {
//             return Ok(())
//         }
//         let mut res = self.0.dispatch(&self.inner().reqs.iter().enumerate().map(|(idx, a)| a.as_ser(idx)).collect::<Vec<_>>()).await?;
//         let text = res.text().await?;
//         let json = match serde_json::from_str::<Vec<RpcResponse<T>>>(&text) {
//             Ok(a) => a,
//             Err(e) => {
//                 anyhow::bail!("{}:\n{}", e, &text)
//             }
//         };
//         let res_res: Result<Vec<(usize, T)>, Error> = json.into_iter().map(|reply| {
//             Ok(match reply.result {
//                 Some(b) => (reply.id.ok_or(anyhow::anyhow!("missing id in response"))?, b),
//                 _ => anyhow::bail!("{:?}", reply.error),
//             })
//         }).collect();
//         let res = res_res?;
//         let req_count = self.inner().reqs.len();
//         let resps_count = self.inner().resps.len();
//         self.inner().resps.extend(std::iter::repeat_with(|| None).take(req_count));
//         for r in res {
//             self.inner().resps[resps_count + r.0] = Some(r.1);
//         }
//         self.inner().reqs = Vec::new();
//         Ok(())
//     }

//     fn send(&mut self) -> Result<Vec<T>, Error> {
//         self.flush()?;
//         let res = std::mem::replace(&mut self.inner().resps, Vec::new());
//         let res = res.into_iter()
//             .map(|r| r.ok_or(anyhow::anyhow!("missing response")))
//             .collect::<Result<Vec<T>, Error>>()?;
//         Ok(res)
//     }
// }

// $(#[$struct_attr])*
// pub struct $struct_name {
//     uri: String,
//     user: Option<String>,
//     pass: Option<String>,
//     max_concurrency: usize,
//     rps: f64,
//     counter: (Mutex<usize>, Condvar),
//     last_req: Mutex<std::time::Instant>,
//     max_batch_size: usize,
//     client: rq::Client,
// }

// impl $struct_name {
//     pub fn new(uri: String, user: Option<String>, pass: Option<String>, max_concurrency: usize, rps: f64, max_batch_size: usize) -> Arc<Self> {
//         Arc::new($struct_name {
//             uri,
//             user,
//             pass,
//             max_concurrency,
//             rps,
//             counter: (Mutex::new(0), Condvar::new()),
//             last_req: Mutex::new(std::time::Instant::now()),
//             max_batch_size,
//             client: rq::Client::new(),
//         })
//     }

//     pub fn batcher<'a, T: for<'de> Deserialize<'de>>(&'a self) -> BatcherPair<'a, T> {
//         BatcherPair(self, ReqBatcher {
//             reqs: Vec::new(),
//             resps: Vec::new(),
//             max_batch_size: self.max_batch_size,
//             phantom: PhantomData,
//         })
//     }

//     async fn call_method<T: Serialize>(&self, method: &'static str, params: T) -> Result<String, Error> {
//         let mut res = self.dispatch(&RpcRequest {
//             method,
//             params,
//         }.polymorphize()).await?;
//         let txt = res.text().await?;
//         Ok(txt)
//     }

// async fn dispatch<T: Serialize>(&self, data: &T) -> Result<rq::Response, Error> {
//     let mut builder = self.client
//         .post(&self.uri);
//     match (&self.user, &self.pass) {
//         (Some(ref u), Some(ref p)) => builder = builder.basic_auth(u, Some(p)),
//         (Some(ref u), None) => builder = builder.basic_auth::<&str, &str>(u, None),
//         _ => (),
//     };
//     builder = builder.json(data);
//     if self.rps > 0.0 {
//         let wait = if self.rps > 1.0 {
//             std::time::Duration::from_secs(1) / self.rps as u32
//         } else {
//              std::time::Duration::from_secs(1) * (1.0 / self.rps) as u32
//         };
//         let mut lock = self.last_req.lock().unwrap();
//         let elapsed = lock.elapsed();
//         if elapsed < wait {
//             std::thread::sleep(wait - elapsed);
//         }
//         *lock = std::time::Instant::now();
//         drop(lock);
//     }
//     if self.max_concurrency > 0 {
//         let mut lock = self.counter.0.lock().unwrap();
//         while *lock == self.max_concurrency {
//             lock = self.counter.1.wait(lock).unwrap();
//         }
//         *lock = *lock + 1;
//         drop(lock);
//     }
//     let res = builder.send();
//     if self.max_concurrency > 0 {
//         let mut lock = self.counter.0.lock().unwrap();
//         *lock = *lock - 1;
//         drop(lock);
//         self.counter.1.notify_one();
//     }
//     let res = res.await?;
//     Ok(res)
// }

//     $(
//         $(
//             $(#[$attr_a])*
//             pub fn $method_a(&self$(, $arg_name_a: $arg_ty_a)*) -> Result<$return_ty_a, Error> {
//                 let txt = self.call_method(stringify!($method_a), ($($arg_name_a,)*))?;
//                 let body: RpcResponse<$return_ty_a> = serde_json::from_str(&txt)
//                     .map_err(|e| anyhow::anyhow!("{}:\n{}", e, &txt))?;
//                 match body.error {
//                     Some(e) => anyhow::bail!("{:?}", e),
//                     None => body.result.ok_or(anyhow::anyhow!("null response")),
//                 }
//             }
//         )*
//         $(
//             $(#[$attr_b])*
//             pub fn $method_b(&self$(, $arg_name_b: $arg_ty_b)*) -> Result<reply::$method_b, Error> {
//                 let txt = self.call_method(stringify!($method_b), ($($arg_name_b,)*))?;
//                 let body: reply::$method_b = (|txt: String| {
//                     $(
//                         match serde_json::from_str::<RpcResponse<$return_ty_b>>(&txt) {
//                             Ok(a) => match a.error {
//                                 Some(e) => anyhow::bail!("{:?}", e),
//                                 None => return Ok(reply::$method_b::$title(a.result.ok_or(anyhow::anyhow!("null response"))?)),
//                             },
//                             Err(_) => (),
//                         };
//                     )+
//                     Err(anyhow::anyhow!("Cannot deserialize to any variant of reply::{}:\n{}", stringify!($method_b), &txt))
//                 })(txt)?;
//                 Ok(body)
//             }
//         )*
//     )*
// }
