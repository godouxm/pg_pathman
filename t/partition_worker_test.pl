use PostgresNode;
use TestLib;

$node_master = get_new_node('master');
$node_master->init(allows_streaming => 1);
$node_master->start;
