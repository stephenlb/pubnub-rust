//! # Request retry policy
//!
//! This module contains the [`RequestRetryPolicy`] struct.
//! It is used to calculate delays between failed requests to the [`PubNub API`]
//! for next retry attempt.
//! It is intended to be used by the [`pubnub`] crate.
//!
//! [`PubNub API`]: https://www.pubnub.com/docs
//! [`pubnub`]: ../index.html
//!
use crate::core::TransportResponse;

/// Request retry policy.
///
///
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
pub enum RequestRetryPolicy {
    /// Requests shouldn't be tried again.
    None,

    /// Retry the request after the same amount of time.
    Linear {
        /// The delay between failed retry attempts.
        delay: u32,

        /// Number of times a request can be retried.
        max_retry: u8,
    },

    /// Retry the request using exponential amount of time.
    Exponential {
        /// Minimum delay between failed retry attempts.
        min_delay: u32,

        /// Maximum delay between failed retry attempts.
        max_delay: u32,

        /// Number of times a request can be retried.
        max_retry: u8,
    },
}

impl RequestRetryPolicy {
    /// Check whether next retry `attempt` is allowed.
    pub(crate) fn retriable(&self, attempt: u8, status_code: u16) -> bool {
        match status_code {
            429 => true,
            500..=599 => match self {
                Self::Linear { max_retry, .. } | Self::Exponential { max_retry, .. } => {
                    attempt.le(max_retry)
                }
                _ => false,
            },
            _ => false,
        }
    }

    #[cfg(feature = "std")]
    #[allow(dead_code)]
    pub(crate) fn retry_delay(&self, attempt: &u8, response: &TransportResponse) -> Option<u32> {
        match response.status {
            // Respect service requested delay.
            429 => (!matches!(self, Self::None))
                .then(|| response.headers.get("retry-after"))
                .flatten()
                .and_then(|value| value.parse::<u32>().ok()),
            500..=599 => match self {
                Self::None => None,
                Self::Linear { delay, .. } => {
                    self.retriable(*attempt, response.status).then_some(*delay)
                }
                Self::Exponential {
                    min_delay,
                    max_delay,
                    ..
                } => self
                    .retriable(*attempt, response.status)
                    .then_some((*min_delay).pow((*attempt).into()).min(*max_delay)),
            },
            _ => None,
        }
    }

    #[cfg(not(feature = "std"))]
    #[allow(dead_code)]
    pub(crate) fn retry_delay(&self, _attempt: &u8, _response: &TransportResponse) -> Option<u32> {
        None
    }
}

impl Default for RequestRetryPolicy {
    fn default() -> Self {
        Self::None
    }
}

#[cfg(test)]
mod should {
    use super::*;
    use crate::lib::collections::HashMap;

    fn client_error_response() -> TransportResponse {
        TransportResponse {
            status: 400,
            ..Default::default()
        }
    }

    fn too_many_requests_error_response() -> TransportResponse {
        TransportResponse {
            status: 429,
            headers: HashMap::from([("retry-after".into(), "150".into())]),
            ..Default::default()
        }
    }

    fn server_error_response() -> TransportResponse {
        TransportResponse {
            status: 500,
            ..Default::default()
        }
    }

    #[test]
    fn create_none_by_default() {
        let policy: RequestRetryPolicy = Default::default();
        assert!(matches!(policy, RequestRetryPolicy::None));
    }

    mod none_policy {
        use super::*;

        #[test]
        fn return_none_delay_for_client_error_response() {
            assert_eq!(
                RequestRetryPolicy::None.retry_delay(&1, &client_error_response()),
                None
            );
        }

        #[test]
        fn return_none_delay_for_server_error_response() {
            assert_eq!(
                RequestRetryPolicy::None.retry_delay(&1, &server_error_response()),
                None
            );
        }

        #[test]
        fn return_none_delay_for_too_many_requests_error_response() {
            assert_eq!(
                RequestRetryPolicy::None.retry_delay(&1, &too_many_requests_error_response()),
                None
            );
        }
    }

    mod linear_policy {
        use super::*;

        #[test]
        fn return_none_delay_for_client_error_response() {
            let policy = RequestRetryPolicy::Linear {
                delay: 10,
                max_retry: 5,
            };

            assert_eq!(policy.retry_delay(&1, &client_error_response()), None);
        }

        #[test]
        fn return_same_delay_for_server_error_response() {
            let expected_delay = 10;
            let policy = RequestRetryPolicy::Linear {
                delay: expected_delay,
                max_retry: 5,
            };

            assert_eq!(
                policy.retry_delay(&1, &server_error_response()),
                Some(expected_delay)
            );

            assert_eq!(
                policy.retry_delay(&2, &server_error_response()),
                Some(expected_delay)
            );
        }

        #[test]
        fn return_none_delay_when_reach_max_retry_for_server_error_response() {
            let expected_delay = 10;
            let policy = RequestRetryPolicy::Linear {
                delay: expected_delay,
                max_retry: 2,
            };

            assert_eq!(
                policy.retry_delay(&2, &server_error_response()),
                Some(expected_delay)
            );

            assert_eq!(policy.retry_delay(&3, &server_error_response()), None);
        }

        #[test]
        fn return_service_delay_for_too_many_requests_error_response() {
            let policy = RequestRetryPolicy::Linear {
                delay: 10,
                max_retry: 2,
            };

            // 150 is from 'server_error_response' `Retry-After` header.
            assert_eq!(
                policy.retry_delay(&2, &too_many_requests_error_response()),
                Some(150)
            );
        }
    }

    mod exponential_policy {
        use super::*;

        #[test]
        fn return_none_delay_for_client_error_response() {
            let expected_delay = 8;
            let policy = RequestRetryPolicy::Exponential {
                min_delay: expected_delay,
                max_delay: 100,
                max_retry: 2,
            };

            assert_eq!(policy.retry_delay(&1, &client_error_response()), None);
        }

        #[test]
        fn return_exponential_delay_for_server_error_response() {
            let expected_delay = 8;
            let policy = RequestRetryPolicy::Exponential {
                min_delay: expected_delay,
                max_delay: 100,
                max_retry: 2,
            };

            assert_eq!(
                policy.retry_delay(&1, &server_error_response()),
                Some(expected_delay)
            );

            assert_eq!(
                policy.retry_delay(&2, &server_error_response()),
                Some(expected_delay.pow(2))
            );
        }

        #[test]
        fn return_none_delay_when_reach_max_retry_for_server_error_response() {
            let expected_delay = 8;
            let policy = RequestRetryPolicy::Exponential {
                min_delay: expected_delay,
                max_delay: 100,
                max_retry: 2,
            };

            assert_eq!(
                policy.retry_delay(&2, &server_error_response()),
                Some(expected_delay.pow(2))
            );

            assert_eq!(policy.retry_delay(&3, &server_error_response()), None);
        }

        #[test]
        fn return_max_delay_when_reach_max_value_for_server_error_response() {
            let expected_delay = 8;
            let max_delay = 50;
            let policy = RequestRetryPolicy::Exponential {
                min_delay: expected_delay,
                max_delay,
                max_retry: 5,
            };

            assert_eq!(
                policy.retry_delay(&1, &server_error_response()),
                Some(expected_delay)
            );

            assert_eq!(
                policy.retry_delay(&2, &server_error_response()),
                Some(max_delay)
            );
        }

        #[test]
        fn return_service_delay_for_too_many_requests_error_response() {
            let policy = RequestRetryPolicy::Exponential {
                min_delay: 10,
                max_delay: 100,
                max_retry: 2,
            };

            // 150 is from 'server_error_response' `Retry-After` header.
            assert_eq!(
                policy.retry_delay(&2, &too_many_requests_error_response()),
                Some(150)
            );
        }
    }
}
