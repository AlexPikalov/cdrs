use types::CBytes;

pub trait Authenticator {
    fn get_auth_token(&self) -> CBytes;
    fn get_cassandra_name(&self) -> &str;
}

#[derive(Clone)]
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

    fn get_cassandra_name(&self) -> &str {
        return "org.apache.cassandra.auth.PasswordAuthenticator";
    }
}
