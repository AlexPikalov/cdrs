
extern crate cdrs;
use cdrs::client::{CDRS, QueryBuilder};
use cdrs::authenticators::PasswordAuthenticator;
use cdrs::compression::Compression;
use cdrs::types::IntoRustByName;

/// this example is to pull employee records from emp table
///
/// # Examples
///
/// ```
/// CREATE KEYSPACE my_namespace WITH REPLICATION = { 'class' : 'SimpleStrategy',
/// 'replication_factor' : 1 };

/// CREATE TABLE emp(
/// emp_id int PRIMARY KEY,
/// emp_name text,
/// emp_city text,
/// emp_sal varint,
/// emp_phone varint
/// );

/// INSERT INTO emp (emp_id, emp_name, emp_city,emp_phone, emp_sal)
/// VALUES(1,'alex', 'NJ', 9848022338, 50000)
/// INSERT INTO emp (emp_id, emp_name, emp_city,emp_phone, emp_sal)
/// VALUES(2,'harry', 'FLL', 9848022338, 50000)
///
///
/// ```
#[derive(Debug,Default)]
struct Employee {
    pub id: i64,
    pub emp_name: String,
    pub emp_city: String,
    pub emp_sal: i64,
    pub emp_phone: i64,
}

fn main() {

    let authenticator = PasswordAuthenticator::new("user", "pass");
    let addr = "127.0.0.1:9042";

    // pass authenticator into CDRS' constructor
    let client = CDRS::new(addr, authenticator).unwrap();

    // start session without compression
    let select_query = QueryBuilder::new("SELECT * FROM my_namespace.emp;").finalize();

    match client.start(Compression::None) {
        Ok(session) => {
            let query_op = session.query(select_query);

            match query_op {
                Ok(res) => {
                    let res_body = res.get_body();
                    if let Some(rows) = res_body.into_rows() {
                        let employees: Vec<Employee> = rows.iter()
                            .map(|row| {
                                let mut employee = Employee { ..Default::default() };
                                if let Some(id) = row.get_by_name("emp_id") {
                                    employee.id = id;
                                }

                                if let Some(emp_name) = row.get_by_name("emp_name") {
                                    employee.emp_name = emp_name;
                                }

                                if let Some(emp_city) = row.get_by_name("emp_city") {
                                    employee.emp_city = emp_city;
                                }

                                if let Some(emp_sal) = row.get_by_name("emp_sal") {
                                    employee.emp_sal = emp_sal;
                                }

                                if let Some(emp_phone) = row.get_by_name("emp_phone") {
                                    employee.emp_phone = emp_phone;
                                }

                                employee
                            })
                            .collect();
                        println!("Employees {:?}", employees);
                    }
                }
                Err(err) => println!("{:?}", err),
            }
        }
        Err(err) => println!("{:?}", err),
    }

}
