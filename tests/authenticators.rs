extern crate cdrs;
use cdrs::authenticators::{Authenticator, PasswordAuthenticator};

#[test]
fn test_password_authenticator_trait_impl() {
    let _ = authenticator_tester(Box::new(PasswordAuthenticator::new("a", "a")));
}

#[test]
fn test_password_authenticator_new() {
    PasswordAuthenticator::new("foo", "bar");
}

#[test]
fn test_password_authenticator_get_cassandra_name() {
    let auth = PasswordAuthenticator::new("foo", "bar");
    assert_eq!(auth.get_cassandra_name(), "org.apache.cassandra.auth.PasswordAuthenticator");
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

fn authenticator_tester(_authenticator: Box<Authenticator>) {}
