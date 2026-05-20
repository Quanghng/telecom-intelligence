import matplotlib.pyplot as plt
import matplotlib.ticker as ticker

plt.style.use("seaborn-v0_8-whitegrid")

hops = [1, 2, 3, 4]

# Context Recall
graphrag_recall = [1.0, 1.0, 1.0, 1.0]
vectorrag_recall = [0.5, 0.0, 0.0, 0.0]

# Latence (secondes)
graphrag_latency = [1.717632, 2.361781, 2.798055, 4.373154]
vectorrag_latency = [1.757479, 1.332888, 1.173984, 1.140978]

fig, (ax1, ax2) = plt.subplots(1, 2, figsize=(12, 5))

# --- Sous-graphique 1 : Context Recall ---
ax1.plot(hops, graphrag_recall, color="royalblue", linestyle="-", marker="o", linewidth=2, markersize=8, label="GraphRAG")
ax1.plot(hops, vectorrag_recall, color="crimson", linestyle="--", marker="s", linewidth=2, markersize=8, label="RAG Vectoriel")
ax1.set_title("Évolution du Context Recall", fontsize=13, fontweight="bold")
ax1.set_xlabel("Profondeur de saut (Hop Depth)", fontsize=11)
ax1.set_ylabel("Context Recall", fontsize=11)
ax1.set_ylim(0.0, 1.1)
ax1.xaxis.set_major_locator(ticker.FixedLocator(hops))
ax1.legend(frameon=True, fontsize=10)

# --- Sous-graphique 2 : Latence ---
ax2.plot(hops, graphrag_latency, color="royalblue", linestyle="-", marker="o", linewidth=2, markersize=8, label="GraphRAG")
ax2.plot(hops, vectorrag_latency, color="crimson", linestyle="--", marker="s", linewidth=2, markersize=8, label="RAG Vectoriel")
ax2.set_title("Évolution de la Latence", fontsize=13, fontweight="bold")
ax2.set_xlabel("Profondeur de saut (Hop Depth)", fontsize=11)
ax2.set_ylabel("Latence (secondes)", fontsize=11)
ax2.set_ylim(0, 5)
ax2.xaxis.set_major_locator(ticker.FixedLocator(hops))
ax2.legend(frameon=True, fontsize=10)

fig.suptitle("Comparaison des performances : GraphRAG vs RAG Vectoriel", fontsize=15, fontweight="bold", y=1.02)
fig.tight_layout()
fig.savefig("ragas_metrics_plots.png", dpi=300, bbox_inches="tight")
print("Figure sauvegardée : ragas_metrics_plots.png")
