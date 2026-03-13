from langgraph.graph import StateGraph

from models import GraphState
from nodes import get_schema_node, transformation_planner_node, code_generator_node, code_validator_node, executor_node, post_validator_node, code_validation_routing, post_validation_routing

graph_builder = StateGraph(GraphState)
graph_builder.add_node("get_schema_node", get_schema_node)
graph_builder.add_node("transformation_planner_node", transformation_planner_node)
graph_builder.add_node("code_generator_node", code_generator_node)
graph_builder.add_node("code_validator_node", code_validator_node)
graph_builder.add_node("executor_node", executor_node)
graph_builder.add_node("post_validator_node", post_validator_node)
graph_builder.set_entry_point("get_schema_node")
graph_builder.add_edge("get_schema_node", "transformation_planner_node")
graph_builder.add_edge("transformation_planner_node", "code_generator_node")
graph_builder.add_edge("code_generator_node", "code_validator_node")
graph_builder.add_conditional_edges("code_validator_node", code_validation_routing)
graph_builder.add_edge("executor_node", "post_validator_node")
graph_builder.add_conditional_edges("post_validator_node", post_validation_routing)
graph = graph_builder.compile()