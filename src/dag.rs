use std::collections::{BTreeSet, HashMap};
use std::hash::Hash;

/// Node in a DAG
#[derive(Debug)]
pub(crate) struct Node<Elem> {
    /// Element of the node, in ROVE's case the name of a QC test
    pub elem: Elem,
    /// QC tests this test depends on
    pub children: BTreeSet<NodeId>,
    /// QC tests that depend on this test
    pub parents: BTreeSet<NodeId>,
}

/// Unique identifier for each node in a DAG
///
/// These are essentially indices of the nodes vector in the DAG
pub(crate) type NodeId = usize;

/// [Directed acyclic graph](https://en.wikipedia.org/wiki/Directed_acyclic_graph)
/// representation
///
/// DAGs are used to define dependencies and pipelines between QC tests in ROVE.
/// Each node in the DAG represents a QC test, and edges between nodes encode
/// dependencies, where the parent node is dependent on the child node.
///
/// The generic parameter `Elem` represents the data held by a node in the graph.
/// For most use cases we expect `&'static str` to work here. Strings
/// containing test names seem a reasonable way to represent QC tests, and these
/// strings can be reasonably expected to be known at compile time, hence
/// `'static`
///
/// The following code sample shows how to construct a DAG:
///
/// ```
/// use rove::Dag;
///
/// let dag = {
///     // create empty dag
///     let mut dag: Dag<&'static str> = Dag::new();
///
///     // add free-standing node
///     let test6 = dag.add_node("test6");
///
///     // add a node with a dependency on the previously defined node
///     let test4 = dag.add_node_with_children("test4", vec![test6]);
///     let test5 = dag.add_node_with_children("test5", vec![test6]);
///
///     let test2 = dag.add_node_with_children("test2", vec![test4]);
///     let test3 = dag.add_node_with_children("test3", vec![test5]);
///
///     let _test1 = dag.add_node_with_children("test1", vec![test2, test3]);
///
///     dag
/// };
///
/// // Resulting dag should look like:
/// //
/// //   6
/// //   ^
/// //  / \
/// // 4   5
/// // ^   ^
/// // |   |
/// // 2   3
/// // ^   ^
/// //  \ /
/// //   1
/// ```
#[derive(Debug)]
pub struct Dag<Elem: Ord + Hash + Clone> {
    /// A vector of all nodes in the graph
    pub(crate) nodes: Vec<Node<Elem>>,
    /// A set of IDs of the nodes that have no parents
    pub(crate) roots: BTreeSet<NodeId>,
    /// A set of IDs of the nodes that have no children
    pub(crate) leaves: BTreeSet<NodeId>,
    /// A hashmap of elements (test names in the case of ROVE) to NodeIds
    ///
    /// This is useful for finding a node in the graph that represents a
    /// certain test, without having to walk the whole nodes vector
    pub(crate) index_lookup: HashMap<Elem, NodeId>,
}

impl<Elem: Ord + Hash + Clone> Node<Elem> {
    fn new(elem: Elem) -> Self {
        Node {
            elem,
            children: BTreeSet::new(),
            parents: BTreeSet::new(),
        }
    }
}

impl<Elem: Ord + Hash + Clone> Dag<Elem> {
    /// Create a new (empty) DAG
    pub fn new() -> Self {
        Dag {
            roots: BTreeSet::new(),
            leaves: BTreeSet::new(),
            nodes: Vec::new(),
            index_lookup: HashMap::new(),
        }
    }

    /// Add a free-standing node to a DAG
    pub fn add_node(&mut self, elem: Elem) -> NodeId {
        let index = self.nodes.len();
        self.nodes.push(Node::new(elem.clone()));

        self.roots.insert(index);
        self.leaves.insert(index);

        self.index_lookup.insert(elem, index);

        index
    }

    /// Add an edge to the DAG. This defines a dependency, where the parent is
    /// dependent on the child
    pub fn add_edge(&mut self, parent: NodeId, child: NodeId) {
        // TODO: we can do better than unwrapping here
        self.nodes.get_mut(parent).unwrap().children.insert(child);
        self.nodes.get_mut(child).unwrap().parents.insert(parent);

        self.roots.remove(&child);
        self.leaves.remove(&parent);
    }

    /// Add a node to the DAG, along with edges representing its dependencies (children)
    pub fn add_node_with_children(&mut self, elem: Elem, children: Vec<NodeId>) -> NodeId {
        let new_node = self.add_node(elem);

        for child in children.into_iter() {
            self.add_edge(new_node, child)
        }

        new_node
    }

    /// Removes an edge from the DAG
    fn remove_edge(&mut self, parent: NodeId, child: NodeId) {
        // TODO: we can do better than unwrapping here
        self.nodes.get_mut(parent).unwrap().children.remove(&child);
        self.nodes.get_mut(child).unwrap().parents.remove(&parent);

        if self.nodes.get(parent).unwrap().children.is_empty() {
            self.leaves.insert(parent);
        }
        if self.nodes.get(child).unwrap().parents.is_empty() {
            self.roots.insert(child);
        }
    }

    #[cfg(test)]
    fn count_edges_iter(&self, curr_node: NodeId, nodes_visited: &mut BTreeSet<NodeId>) -> u32 {
        let mut edge_count = 0;

        for child in self.nodes.get(curr_node).unwrap().children.iter() {
            edge_count += 1;

            if !nodes_visited.contains(child) {
                edge_count += self.count_edges_iter(*child, nodes_visited);
            }
        }

        nodes_visited.insert(curr_node);

        edge_count
    }

    /// Counts the number of edges in the DAG
    #[cfg(test)]
    pub fn count_edges(&self) -> u32 {
        let mut edge_count = 0;
        let mut nodes_visited: BTreeSet<NodeId> = BTreeSet::new();

        for root in self.roots.iter() {
            edge_count += self.count_edges_iter(*root, &mut nodes_visited);
        }

        edge_count
    }

    fn recursive_parent_remove(&mut self, parent: NodeId, child: NodeId) {
        self.remove_edge(parent, child);
        for granchild in self.nodes.get(child).unwrap().children.clone().iter() {
            self.recursive_parent_remove(parent, *granchild);
        }
    }

    fn transitive_reduce_iter(&mut self, curr_node: NodeId) {
        let children = self.nodes.get(curr_node).unwrap().children.clone(); // FIXME: would be nice to not have to clone here

        for child in children.iter() {
            for granchild in self.nodes.get(*child).unwrap().children.clone().iter() {
                self.recursive_parent_remove(curr_node, *granchild);
            }
        }

        for child in children.iter() {
            self.transitive_reduce_iter(*child);
        }
    }

    /// Performs a [transitive reduction](https://en.wikipedia.org/wiki/Transitive_reduction)
    /// on the DAG
    ///
    /// This essentially removes any redundant dependencies in the graph
    pub fn transitive_reduce(&mut self) {
        for root in self.roots.clone().iter() {
            self.transitive_reduce_iter(*root)
        }
    }

    fn cycle_check_iter(&self, curr_node: NodeId, ancestors: &mut Vec<NodeId>) -> bool {
        if ancestors.contains(&curr_node) {
            return true;
        }

        ancestors.push(curr_node);

        for child in self.nodes.get(curr_node).unwrap().children.iter() {
            if self.cycle_check_iter(*child, ancestors) {
                return true;
            }
        }

        ancestors.pop();

        false
    }

    /// Check for cycles in the DAG
    ///
    /// This can be used to validate a DAG, as a DAG **must not** contain cycles.
    /// Returns true if a cycle is detected, false otherwise.
    pub fn cycle_check(&self) -> bool {
        let mut ancestors: Vec<NodeId> = Vec::new();

        for root in self.roots.iter() {
            if self.cycle_check_iter(*root, &mut ancestors) {
                return true;
            }
        }

        false
    }
}

impl<Elem: Ord + Hash + Clone> Default for Dag<Elem> {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_transitive_reduce() {
        let mut dag: Dag<u32> = Dag::new();

        let node1 = dag.add_node(1);
        let node2 = dag.add_node(2);
        let node3 = dag.add_node(3);
        let node4 = dag.add_node(4);
        let node5 = dag.add_node(5);

        dag.add_edge(node1, node2);
        dag.add_edge(node1, node3);
        dag.add_edge(node1, node4);
        dag.add_edge(node1, node5);

        dag.add_edge(node2, node4);
        dag.add_edge(node3, node4);
        dag.add_edge(node3, node5);
        dag.add_edge(node4, node5);

        assert_eq!(dag.count_edges(), 8);
        assert!(dag.nodes.get(node1).unwrap().children.contains(&node4));
        assert!(dag.nodes.get(node1).unwrap().children.contains(&node5));
        assert!(dag.nodes.get(node3).unwrap().children.contains(&node5));

        dag.transitive_reduce();

        assert_eq!(dag.count_edges(), 5);
        assert!(!dag.nodes.get(node1).unwrap().children.contains(&node4));
        assert!(!dag.nodes.get(node1).unwrap().children.contains(&node5));
        assert!(!dag.nodes.get(node3).unwrap().children.contains(&node5));
    }

    #[test]
    fn test_cycle_check() {
        let mut good_dag: Dag<u32> = Dag::new();

        let node1 = good_dag.add_node(1);
        let node2 = good_dag.add_node(2);
        let node3 = good_dag.add_node(3);
        let node4 = good_dag.add_node(4);

        good_dag.add_edge(node1, node2);
        good_dag.add_edge(node1, node3);
        good_dag.add_edge(node2, node4);
        good_dag.add_edge(node3, node4);

        assert!(!good_dag.cycle_check());

        let mut bad_dag: Dag<u32> = Dag::new();

        let node1 = bad_dag.add_node(1);
        let node2 = bad_dag.add_node(2);
        let node3 = bad_dag.add_node(3);
        let node4 = bad_dag.add_node(4);

        bad_dag.add_edge(node1, node2);
        bad_dag.add_edge(node1, node3);
        bad_dag.add_edge(node2, node4);
        bad_dag.add_edge(node4, node3);
        bad_dag.add_edge(node3, node2);

        assert!(bad_dag.cycle_check());
    }
}
