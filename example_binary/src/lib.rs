use dagmar::Dag;

pub fn construct_hardcoded_dag() -> Dag<String> {
    let mut dag: Dag<String> = Dag::new();

    dag.add_node(String::from("dip_check"));
    dag.add_node(String::from("step_check"));
    dag.add_node(String::from("buddy_check"));
    dag.add_node(String::from("sct"));

    dag
}
