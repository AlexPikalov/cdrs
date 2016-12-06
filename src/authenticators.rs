use types::CBytes;

pub trait Authenticator {
    fn get_auth_token(&self) -> CBytes;
    fn get_cassandra_name(&self) -> &str;
}

pub struct PasswordAuthenticator {
    username: String,
    password: String
}

impl PasswordAuthenticator {
    pub fn new(username: String, password: String) -> PasswordAuthenticator {
        return PasswordAuthenticator {
            username: username,
            password: password
        };
    }
}

impl Authenticator for PasswordAuthenticator {
    fn get_auth_token(&self) -> CBytes {
        let mut token = vec![0];
        token.extend_from_slice(self.username.clone().into_bytes().as_slice());
        token.push(0);
        token.extend_from_slice(self.password.clone().into_bytes().as_slice());

        return CBytes::new(token);
    }

    fn get_cassandra_name(&self) -> &str {
        return "org.apache.cassandra.auth.PasswordAuthenticator";
    }
}
