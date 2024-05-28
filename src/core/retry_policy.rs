//! # Request retry policy
//!
//! This module contains the [`RequestRetryConfiguration`] struct.
//! It is used to calculate delays between failed requests to the [`PubNub API`]
//! for next retry attempt.
//! It is intended to be used by the [`pubnub`] crate.
//!
//! [`PubNub API`]: https://www.pubnub.com/docs
//! [`pubnub`]: ../index.html

use getrandom::getrandom;

use crate::{core::PubNubError, lib::alloc::vec::Vec};

/// List of known endpoint groups (by context)
#[derive(Debug, Copy, Clone, PartialEq, Eq)]
pub enum Endpoint {
    /// Unknown endpoint.
    Unknown,

    /// The endpoints to send messages.
    MessageSend,

    /// The endpoint for real-time update retrieval.
    Subscribe,

    /// The endpoint to access and manage `user_id` presence and fetch channel
    /// presence information.
    Presence,

    /// The endpoint to access and manage files in channel-specific storage.
    Files,

    /// The endpoint to access and manage messages for a specific channel(s) in
    /// the persistent storage.
    MessageStorage,

    /// The endpoint to access and manage channel groups.
    ChannelGroups,

    /// The endpoint to access and manage device registration for channel push
    /// notifications.
    DevicePushNotifications,

    /// The endpoint to access and manage App Context objects.
    AppContext,

    /// The endpoint to access and manage reactions for a specific message.
    MessageReactions,
}

/// Request retry policy.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum RequestRetryConfiguration {
    /// Requests shouldn't be tried again.
    None,

    /// Retry the request after the same amount of time.
    Linear {
        /// The delay between failed retry attempts in seconds.
        delay: u64,

        /// Number of times a request can be retried.
        max_retry: u8,

        /// Optional list of excluded endpoint groups.
        ///
        /// Endpoint groups for which automatic retry shouldn't be used.
        excluded_endpoints: Option<Vec<Endpoint>>,
    },

    /// Retry the request using exponential amount of time.
    Exponential {
        /// Minimum delay between failed retry attempts in seconds.
        min_delay: u64,

        /// Maximum delay between failed retry attempts in seconds.
        max_delay: u64,

        /// Number of times a request can be retried.
        max_retry: u8,

        /// Optional list of excluded endpoint groups.
        ///
        /// Endpoint groups for which automatic retry shouldn't be used.
        excluded_endpoints: Option<Vec<Endpoint>>,
    },
}

impl RequestRetryConfiguration {
    /// Creates a new instance of the `RequestRetryConfiguration` enum with a
    /// default linear policy.
    ///
    /// The `Linear` policy retries the operation with a fixed delay between
    /// each retry. The default delay is 2 seconds and the default maximum
    /// number of retries is 10.
    ///
    /// # Example
    ///
    /// ```
    /// use pubnub::{Keyset, PubNubClientBuilder, RequestRetryConfiguration};
    ///
    /// # fn main() -> Result<(), Box<dyn std::error::Error>> {
    /// let retry_configuration = RequestRetryConfiguration::default_linear();
    /// let client = PubNubClientBuilder::with_reqwest_transport()
    ///     .with_keyset(Keyset {
    ///         publish_key: Some("pub-c-abc123"),
    ///         subscribe_key: "sub-c-abc123",
    ///         secret_key: None,
    ///     })
    ///     .with_user_id("my-user-id")
    ///     .with_retry_configuration(retry_configuration)
    ///     .build()?;
    /// #    Ok(())
    /// # }
    /// ```
    ///
    /// # Returns
    ///
    /// A new instance of the `RetryPolicy` enum with the default linear policy.
    pub fn default_linear() -> Self {
        Self::Linear {
            delay: 2,
            max_retry: 10,
            excluded_endpoints: None,
        }
    }

    /// Creates a new instance of the `RequestRetryConfiguration` enum with a
    /// default exponential backoff policy.
    ///
    /// The `Exponential` backoff policy increases the delay between retries
    /// exponentially. It starts with a minimum delay of 2 seconds and a
    /// maximum delay of 150 seconds. It allows a maximum number of 6
    /// retries.
    ///
    /// # Example
    ///
    /// ```
    /// use pubnub::{Keyset, PubNubClientBuilder, RequestRetryConfiguration};
    ///
    /// # fn main() -> Result<(), Box<dyn std::error::Error>> {
    /// let retry_configuration = RequestRetryConfiguration::default_exponential();
    /// let client = PubNubClientBuilder::with_reqwest_transport()
    ///     .with_keyset(Keyset {
    ///         publish_key: Some("pub-c-abc123"),
    ///         subscribe_key: "sub-c-abc123",
    ///         secret_key: None,
    ///     })
    ///     .with_user_id("my-user-id")
    ///     .with_retry_configuration(retry_configuration)
    ///     .build()?;
    /// #    Ok(())
    /// # }
    /// ```
    pub fn default_exponential() -> Self {
        Self::Exponential {
            min_delay: 2,
            max_delay: 150,
            max_retry: 6,
            excluded_endpoints: None,
        }
    }

    /// Check whether next retry `attempt` is allowed.
    ///
    /// # Arguments
    ///
    /// * `path` - Optional path of the failed request.
    /// * `attempt` - The attempt count of the request.
    /// * `error` - An optional `PubNubError` representing the error response.
    ///   If `None`, the request cannot be retried.
    ///
    /// # Returns
    ///
    /// `true` if it is allowed to retry request one more time.
    pub(crate) fn retriable<S>(
        &self,
        path: Option<S>,
        attempt: &u8,
        error: Option<&PubNubError>,
    ) -> bool
    where
        S: Into<String>,
    {
        if self.is_excluded_endpoint(path)
            || self.reached_max_retry(attempt)
            || matches!(self, RequestRetryConfiguration::None)
        {
            return false;
        }

        error
            .and_then(|e| e.transport_response())
            .map(|response| matches!(response.status, 429 | 500..=599))
            .unwrap_or(false)
    }

    /// Calculate the delay before retrying a request.
    ///
    /// If the request can be retried based on the given attempt count and
    /// error, the delay is calculated based on the error response status code.
    /// - If the status code is 429 (Too Many Requests), the delay is determined
    ///   by the `retry-after` header in the response, if present.
    /// - If the status code is in the range 500-599 (Server Error), the delay
    ///   is calculated based on the configured retry strategy.
    ///
    /// # Arguments
    ///
    /// * `path` - Optional path of the failed request.
    /// * `attempt` - The attempt count of the request.
    /// * `error` - An optional `PubNubError` representing the error response.
    ///   If `None`, the request cannot be retried.
    ///
    /// # Returns
    ///
    /// An optional `u64` representing the delay in microseconds before retrying
    /// the request. `None` if the request should not be retried.
    pub(crate) fn retry_delay(
        &self,
        path: Option<String>,
        attempt: &u8,
        error: Option<&PubNubError>,
    ) -> Option<u64> {
        if !self.retriable(path, attempt, error) {
            return None;
        }

        error
            .and_then(|err| err.transport_response())
            .map(|response| match response.status {
                // Respect service requested delay.
                429 if response.headers.contains_key("retry-after") => {
                    (!matches!(self, Self::None))
                        .then(|| response.headers.get("retry-after"))
                        .flatten()
                        .and_then(|value| value.parse::<u64>().ok())
                }
                500..=599 => match self {
                    Self::None => None,
                    Self::Linear { delay, .. } => Some(*delay),
                    Self::Exponential {
                        min_delay,
                        max_delay,
                        ..
                    } => Some((*min_delay * 2_u64.pow((*attempt - 1) as u32)).min(*max_delay)),
                },
                _ => None,
            })
            .map(Self::delay_in_microseconds)
            .unwrap_or(None)
    }

    /// Check whether failed endpoint has been excluded or not.
    ///
    /// # Arguments
    ///
    /// * `path` - Optional path of the failed request.
    ///
    /// # Returns
    ///
    /// `true` in case if endpoint excluded from retry list or no path passed.
    fn is_excluded_endpoint<S>(&self, path: Option<S>) -> bool
    where
        S: Into<String>,
    {
        let Some(path) = path.map(|p| Endpoint::from(p.into())) else {
            return false;
        };

        let Some(excluded_endpoints) = (match self {
            Self::Linear {
                excluded_endpoints, ..
            }
            | Self::Exponential {
                excluded_endpoints, ..
            } => excluded_endpoints,
            _ => &None,
        }) else {
            return false;
        };

        excluded_endpoints.contains(&path)
    }

    /// Check whether reached maximum retry count or not.
    fn reached_max_retry(&self, attempt: &u8) -> bool {
        match self {
            Self::Linear { max_retry, .. } | Self::Exponential { max_retry, .. } => {
                attempt.gt(max_retry)
            }
            _ => false,
        }
    }

    /// Calculates the delay in microseconds given a delay in seconds.
    ///
    /// # Arguments
    ///
    /// * `delay_in_seconds` - The delay in seconds. If `None`, returns `None`.
    ///
    /// # Returns
    ///
    /// * `Some(delay_in_microseconds)` - The delay in microseconds.
    /// * `None` - If `delay_in_seconds` is `None`.
    fn delay_in_microseconds(delay_in_seconds: Option<u64>) -> Option<u64> {
        let delay_in_seconds = delay_in_seconds?;

        const MICROS_IN_SECOND: u64 = 1_000_000;
        let delay = delay_in_seconds * MICROS_IN_SECOND;
        let mut random_bytes = [0u8; 8];

        if getrandom(&mut random_bytes).is_err() {
            return Some(delay);
        }

        Some(delay + u64::from_be_bytes(random_bytes) % MICROS_IN_SECOND)
    }
}

impl Default for RequestRetryConfiguration {
    fn default() -> Self {
        Self::None
    }
}

impl From<String> for Endpoint {
    fn from(value: String) -> Self {
        match value.as_str() {
            path if path.starts_with("/v2/subscribe") => Endpoint::Subscribe,
            path if path.starts_with("/publish/") || path.starts_with("/signal/") => {
                Endpoint::MessageSend
            }
            path if path.starts_with("/v2/presence") => Endpoint::Presence,
            path if path.starts_with("/v2/history/") || path.starts_with("/v3/history/") => {
                Endpoint::MessageStorage
            }
            path if path.starts_with("/v1/message-actions/") => Endpoint::MessageReactions,
            path if path.starts_with("/v1/channel-registration/") => Endpoint::ChannelGroups,
            path if path.starts_with("/v2/objects/") => Endpoint::AppContext,
            path if path.starts_with("/v1/push/") || path.starts_with("/v2/push/") => {
                Endpoint::DevicePushNotifications
            }
            path if path.starts_with("/v1/files/") => Endpoint::Files,
            _ => Endpoint::Unknown,
        }
    }
}

#[cfg(test)]
mod should {
    use super::*;
    use crate::{
        core::TransportResponse,
        lib::{alloc::boxed::Box, collections::HashMap},
    };

    fn client_error_response() -> TransportResponse {
        TransportResponse {
            status: 400,
            ..Default::default()
        }
    }

    fn too_many_requests_error_response() -> TransportResponse {
        TransportResponse {
            status: 429,
            headers: HashMap::from([(String::from("retry-after"), String::from("150"))]),
            ..Default::default()
        }
    }

    fn server_error_response() -> TransportResponse {
        TransportResponse {
            status: 500,
            ..Default::default()
        }
    }

    fn is_equal_with_accuracy(lhv: Option<u64>, rhv: Option<u64>) -> bool {
        if lhv.is_none() && rhv.is_none() {
            return true;
        }

        let Some(lhv) = lhv else { return false };
        let Some(rhv) = rhv else { return false };

        if !(rhv * 1_000_000..=rhv * 1_000_000 + 999_999).contains(&lhv) {
            panic!(
                "{lhv} is not within expected range {}..{}",
                rhv * 1_000_000,
                rhv * 1_000_000 + 999_999
            )
        }

        true
    }

    #[test]
    fn create_none_by_default() {
        let policy: RequestRetryConfiguration = Default::default();
        assert!(matches!(policy, RequestRetryConfiguration::None));
    }

    mod none_policy {
        use super::*;

        #[test]
        fn return_none_delay_for_client_error_response() {
            assert_eq!(
                RequestRetryConfiguration::None.retry_delay(
                    None,
                    &1,
                    Some(&PubNubError::general_api_error(
                        "test",
                        None,
                        Some(Box::new(client_error_response()))
                    ))
                ),
                None
            );
        }

        #[test]
        fn return_none_delay_for_server_error_response() {
            assert_eq!(
                RequestRetryConfiguration::None.retry_delay(
                    None,
                    &1,
                    Some(&PubNubError::general_api_error(
                        "test",
                        None,
                        Some(Box::new(server_error_response()))
                    ))
                ),
                None
            );
        }

        #[test]
        fn return_none_delay_for_too_many_requests_error_response() {
            assert_eq!(
                RequestRetryConfiguration::None.retry_delay(
                    None,
                    &1,
                    Some(&PubNubError::general_api_error(
                        "test",
                        None,
                        Some(Box::new(too_many_requests_error_response()))
                    ))
                ),
                None
            );
        }
    }

    mod linear_policy {
        use super::*;

        #[test]
        fn return_none_delay_for_client_error_response() {
            let policy = RequestRetryConfiguration::Linear {
                delay: 10,
                max_retry: 5,
                excluded_endpoints: None,
            };

            assert_eq!(
                policy.retry_delay(
                    None,
                    &1,
                    Some(&PubNubError::general_api_error(
                        "test",
                        None,
                        Some(Box::new(client_error_response()))
                    ))
                ),
                None
            );
        }

        #[test]
        fn return_same_delay_for_server_error_response() {
            let expected_delay: u64 = 10;
            let policy = RequestRetryConfiguration::Linear {
                delay: expected_delay,
                max_retry: 5,
                excluded_endpoints: None,
            };

            assert!(is_equal_with_accuracy(
                policy.retry_delay(
                    None,
                    &1,
                    Some(&PubNubError::general_api_error(
                        "test",
                        None,
                        Some(Box::new(server_error_response()))
                    ))
                ),
                Some(expected_delay)
            ));

            assert!(is_equal_with_accuracy(
                policy.retry_delay(
                    None,
                    &2,
                    Some(&PubNubError::general_api_error(
                        "test",
                        None,
                        Some(Box::new(server_error_response()))
                    ))
                ),
                Some(expected_delay)
            ));
        }

        #[test]
        fn return_none_delay_when_reach_max_retry_for_server_error_response() {
            let expected_delay: u64 = 10;
            let policy = RequestRetryConfiguration::Linear {
                delay: expected_delay,
                max_retry: 3,
                excluded_endpoints: None,
            };

            assert!(is_equal_with_accuracy(
                policy.retry_delay(
                    None,
                    &2,
                    Some(&PubNubError::general_api_error(
                        "test",
                        None,
                        Some(Box::new(server_error_response()))
                    ))
                ),
                Some(expected_delay)
            ));

            assert_eq!(
                policy.retry_delay(
                    None,
                    &4,
                    Some(&PubNubError::general_api_error(
                        "test",
                        None,
                        Some(Box::new(server_error_response()))
                    ))
                ),
                None
            );
        }

        #[test]
        fn return_service_delay_for_too_many_requests_error_response() {
            let policy = RequestRetryConfiguration::Linear {
                delay: 10,
                max_retry: 2,
                excluded_endpoints: None,
            };

            // 150 is from 'server_error_response' `Retry-After` header.
            assert!(is_equal_with_accuracy(
                policy.retry_delay(
                    None,
                    &2,
                    Some(&PubNubError::general_api_error(
                        "test",
                        None,
                        Some(Box::new(too_many_requests_error_response()))
                    ))
                ),
                Some(150)
            ));
        }
    }

    mod exponential_policy {
        use super::*;

        #[test]
        fn return_none_delay_for_client_error_response() {
            let expected_delay = 8;
            let policy = RequestRetryConfiguration::Exponential {
                min_delay: expected_delay,
                max_delay: 100,
                max_retry: 2,
                excluded_endpoints: None,
            };

            assert_eq!(
                policy.retry_delay(
                    None,
                    &1,
                    Some(&PubNubError::general_api_error(
                        "test",
                        None,
                        Some(Box::new(client_error_response()))
                    ))
                ),
                None
            );
        }

        #[test]
        fn return_exponential_delay_for_server_error_response() {
            let expected_delay = 8;
            let policy = RequestRetryConfiguration::Exponential {
                min_delay: expected_delay,
                max_delay: 100,
                max_retry: 3,
                excluded_endpoints: None,
            };

            assert!(is_equal_with_accuracy(
                policy.retry_delay(
                    None,
                    &1,
                    Some(&PubNubError::general_api_error(
                        "test",
                        None,
                        Some(Box::new(server_error_response()))
                    ))
                ),
                Some(expected_delay)
            ));

            assert!(is_equal_with_accuracy(
                policy.retry_delay(
                    None,
                    &2,
                    Some(&PubNubError::general_api_error(
                        "test",
                        None,
                        Some(Box::new(server_error_response()))
                    ))
                ),
                Some(expected_delay * 2_u64.pow(2 - 1))
            ));
        }

        #[test]
        fn return_none_delay_when_reach_max_retry_for_server_error_response() {
            let expected_delay = 8;
            let policy = RequestRetryConfiguration::Exponential {
                min_delay: expected_delay,
                max_delay: 100,
                max_retry: 3,
                excluded_endpoints: None,
            };

            assert!(is_equal_with_accuracy(
                policy.retry_delay(
                    None,
                    &2,
                    Some(&PubNubError::general_api_error(
                        "test",
                        None,
                        Some(Box::new(server_error_response()))
                    ))
                ),
                Some(expected_delay * 2_u64.pow(2 - 1))
            ));

            assert_eq!(
                policy.retry_delay(
                    None,
                    &4,
                    Some(&PubNubError::general_api_error(
                        "test",
                        None,
                        Some(Box::new(server_error_response()))
                    ))
                ),
                None
            );
        }

        #[test]
        fn return_max_delay_when_reach_max_value_for_server_error_response() {
            let expected_delay = 8;
            let max_delay = 50;
            let policy = RequestRetryConfiguration::Exponential {
                min_delay: expected_delay,
                max_delay,
                max_retry: 5,
                excluded_endpoints: None,
            };

            assert!(is_equal_with_accuracy(
                policy.retry_delay(
                    None,
                    &1,
                    Some(&PubNubError::general_api_error(
                        "test",
                        None,
                        Some(Box::new(server_error_response()))
                    ))
                ),
                Some(expected_delay)
            ));

            assert!(is_equal_with_accuracy(
                policy.retry_delay(
                    None,
                    &4,
                    Some(&PubNubError::general_api_error(
                        "test",
                        None,
                        Some(Box::new(server_error_response()))
                    ))
                ),
                Some(max_delay)
            ));
        }

        #[test]
        fn return_service_delay_for_too_many_requests_error_response() {
            let policy = RequestRetryConfiguration::Exponential {
                min_delay: 10,
                max_delay: 100,
                max_retry: 2,
                excluded_endpoints: None,
            };

            // 150 is from 'server_error_response' `Retry-After` header.
            assert!(is_equal_with_accuracy(
                policy.retry_delay(
                    None,
                    &2,
                    Some(&PubNubError::general_api_error(
                        "test",
                        None,
                        Some(Box::new(too_many_requests_error_response()))
                    ))
                ),
                Some(150)
            ));
        }
    }
}
