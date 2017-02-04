extern crate cdrs;
use cdrs::authenticators::{Authenticator, PasswordAuthenticator,AuthenticatorNone};

#[test]
fn test_password_authenticator_trait_impl() {
    let authenticator = PasswordAuthenticator::new("a", "a");
    let _ = authenticator_tester(Box::new(authenticator));
}

#[test]
fn test_password_authenticator_new() {
    PasswordAuthenticator::new("foo", "bar");
}

#[test]
fn test_password_authenticator_get_cassandra_name() {
    let auth = PasswordAuthenticator::new("foo", "bar");
    assert_eq!(auth.get_cassandra_name(), Some("org.apache.cassandra.auth.PasswordAuthenticator"));
}

#[test]
fn test_password_authenticator_get_auth_token() {
    let auth = PasswordAuthenticator::new("foo", "bar");
    let mut expected_token = vec![0];
    expected_token.extend_from_slice("foo".as_bytes());
    expected_token.push(0);
    expected_token.extend_from_slice("bar".as_bytes());

    assert_eq!(auth.get_auth_token().into_plain(), expected_token);
}

#[test]
fn test_authenticator_none_get_cassandra_name() {
    let auth = AuthenticatorNone;
    assert_eq!(auth.get_cassandra_name(), None);
    assert_eq!(auth.get_auth_token().into_plain(), vec![0]);
}

fn authenticator_tester<A: Authenticator>(_authenticator: Box<A>) {}
