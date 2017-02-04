use types::CBytes;

pub trait Authenticator: Clone {
    fn get_auth_token(&self) -> CBytes;
    fn get_cassandra_name(&self) -> Option<&str>;
}

#[derive(Debug, Clone)]
pub struct PasswordAuthenticator<'a> {
    username: &'a str,
    password: &'a str
}

impl<'a> PasswordAuthenticator<'a> {
    pub fn new<'b>(username: &'b str, password: &'b str) -> PasswordAuthenticator<'b> {
        return PasswordAuthenticator {
            username: username,
            password: password
        };
    }
}

impl<'a> Authenticator for PasswordAuthenticator<'a> {
    fn get_auth_token(&self) -> CBytes {
        let mut token = vec![0];
        token.extend_from_slice(self.username.as_bytes());
        token.push(0);
        token.extend_from_slice(self.password.as_bytes());

        return CBytes::new(token);
    }

    fn get_cassandra_name(&self) -> Option<&str> {
        return Some("org.apache.cassandra.auth.PasswordAuthenticator");
    }
}

#[derive(Debug, Clone)]
pub struct AuthenticatorNone;


impl Authenticator for AuthenticatorNone {
    fn get_auth_token(&self) -> CBytes {
        return CBytes::new(vec![0]);
    }

    fn get_cassandra_name(&self) -> Option<&str> {
        return None;
    }

}
