mod common;

#[cfg(feature = "e2e-tests")]
use cassandra_protocol::envelope::Version;
#[cfg(feature = "e2e-tests")]
use cdrs_tokio::query_values;
#[cfg(feature = "e2e-tests")]
use cdrs_tokio::types::IntoRustByName;
#[cfg(feature = "e2e-tests")]
use common::*;

#[tokio::test]
#[cfg(feature = "e2e-tests")]
async fn query_values_in_v4() {
    let cql = "CREATE TABLE IF NOT EXISTS cdrs_test.test_query_values_in \
             (id text PRIMARY KEY)";
    let session = setup(cql, Version::V4).await.expect("setup");

    query_values_in_test(cql, session).await;
}

#[tokio::test]
#[cfg(feature = "e2e-tests")]
async fn query_values_in_v5() {
    let cql = "CREATE TABLE IF NOT EXISTS cdrs_test.test_query_values_in \
             (id text PRIMARY KEY)";
    let session = setup(cql, Version::V5).await.expect("setup");

    query_values_in_test(cql, session).await;
}

#[cfg(feature = "e2e-tests")]
async fn query_values_in_test(cql: &str, session: CurrentSession) {
    session.query(cql).await.expect("create table error");

    let query_insert = "INSERT INTO cdrs_test.test_query_values_in \
                      (id) VALUES (?)";

    let items = vec!["1".to_string(), "2".to_string(), "3".to_string()];

    for item in items {
        let values = query_values!(item);
        session
            .query_with_values(query_insert, values)
            .await
            .expect("insert item error");
    }

    let cql = "SELECT * FROM cdrs_test.test_query_values_in WHERE id IN ?";
    let criteria = vec!["1".to_string(), "3".to_string()];

    let rows = session
        .query_with_values(cql, query_values!(criteria.clone()))
        .await
        .expect("select values query error")
        .response_body()
        .expect("get body error")
        .into_rows()
        .expect("converting into rows error");

    assert_eq!(rows.len(), criteria.len());

    let found_all_matching_criteria = criteria.iter().all(|criteria_item: &String| {
        rows.iter().any(|row| {
            let id: String = row.get_r_by_name("id").expect("id");

            criteria_item.clone() == id
        })
    });

    assert!(
        found_all_matching_criteria,
        "should find at least one element for each criteria"
    );
}
