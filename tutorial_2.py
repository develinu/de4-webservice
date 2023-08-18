import streamlit as st
import pandas as pd
import numpy as np
import graphviz


# chart_data = pd.DataFrame(
#     np.random.randn(20, 3),
#     columns=["a", "b", "c"]
# )
#
# st.write(chart_data)
#
# st.line_chart(chart_data)
# st.area_chart(chart_data)
# st.bar_chart(chart_data)
#
# geo_df = pd.DataFrame(
#     np.random.randn(1000, 2) / [50, 50] + [37.76, -122.4],
#     columns=["lat", "lon"]
# )
#
# st.write(geo_df)
# st.map(geo_df)

graph = graphviz.Digraph()
graph.edge("a1", "a2")
graph.edge("a2", "a3")
graph.edge("a3", "a4")
graph.edge("a4", "a1")
st.graphviz_chart(graph)

