import matplotlib.pyplot as plt
import matplotlib.ticker as ticker
import numpy as np

plt.style.use("seaborn-v0_8-whitegrid")

hops = [1, 2, 3, 4]

# ── Metrics (from scored_evaluation_results_hop_N.csv) ─────────────────────
graphrag_faithfulness  = [1.000, 1.000, 1.000, 0.944]
vectorrag_faithfulness = [0.571, 0.714, 0.941, 1.000]

graphrag_relevancy     = [0.953, 0.712, 0.849, 0.593]
vectorrag_relevancy    = [0.958, 0.855, 0.630, 0.000]

graphrag_precision     = [1.000, 1.000, 1.000, 1.000]
vectorrag_precision    = [0.000, 0.500, 0.000, 0.000]

graphrag_recall        = [1.000, 1.000, 1.000, 1.000]
vectorrag_recall       = [0.500, 0.000, 0.000, 0.000]

graphrag_latency       = [1.972, 1.241, 2.138, 2.852]
vectorrag_latency      = [0.648, 0.768, 0.705, 1.063]

# ── Colour palette ──────────────────────────────────────────────────────────
BLUE   = "#2563EB"
RED    = "#DC2626"
ALPHA  = 0.15        # fill band alpha

# ── Helper ──────────────────────────────────────────────────────────────────
def _style_ax(ax, title, ylabel, ylim=(0.0, 1.09)):
    ax.set_title(title, fontsize=12, fontweight="bold", pad=8)
    ax.set_xlabel("Hop Depth", fontsize=10)
    ax.set_ylabel(ylabel, fontsize=10)
    ax.set_ylim(*ylim)
    ax.xaxis.set_major_locator(ticker.FixedLocator(hops))
    ax.legend(frameon=True, fontsize=9)
    ax.tick_params(labelsize=9)

def _plot_pair(ax, gr, vr, label_gr="GraphRAG", label_vr="VectorRAG"):
    ax.plot(hops, gr, color=BLUE,  linestyle="-",  marker="o", linewidth=2,
            markersize=7, label=label_gr)
    ax.plot(hops, vr, color=RED,   linestyle="--", marker="s", linewidth=2,
            markersize=7, label=label_vr)
    ax.fill_between(hops, gr, vr,
                    where=[g >= v for g, v in zip(gr, vr)],
                    alpha=ALPHA, color=BLUE, interpolate=True)
    ax.fill_between(hops, gr, vr,
                    where=[g < v for g, v in zip(gr, vr)],
                    alpha=ALPHA, color=RED, interpolate=True)

# ── Figure layout: 2 rows × 3 cols (all cells equal size) ─────────────────
fig = plt.figure(figsize=(16, 10))
gs  = fig.add_gridspec(2, 3, hspace=0.45, wspace=0.35)

ax_faith  = fig.add_subplot(gs[0, 0])
ax_rel    = fig.add_subplot(gs[0, 1])
ax_prec   = fig.add_subplot(gs[0, 2])
ax_rec    = fig.add_subplot(gs[1, 0])
ax_lat    = fig.add_subplot(gs[1, 1])
ax_sum    = fig.add_subplot(gs[1, 2])

# ── Plot each metric ─────────────────────────────────────────────────────────
_plot_pair(ax_faith, graphrag_faithfulness,  vectorrag_faithfulness)
_style_ax(ax_faith, "Faithfulness", "Score")

_plot_pair(ax_rel,   graphrag_relevancy,     vectorrag_relevancy)
_style_ax(ax_rel,   "Answer Relevancy",     "Score")

_plot_pair(ax_prec,  graphrag_precision,     vectorrag_precision)
_style_ax(ax_prec,  "Context Precision",    "Score")

_plot_pair(ax_rec,   graphrag_recall,        vectorrag_recall)
_style_ax(ax_rec,   "Context Recall",       "Score")

# ── Latency (different y-axis range) ─────────────────────────────────────────
_plot_pair(ax_lat, graphrag_latency, vectorrag_latency)
_style_ax(ax_lat, "Latency (seconds) — lower is better",
          "Latency (s)", ylim=(0, 4.0))

# Annotate trade-off
ax_lat.annotate(
    "GraphRAG: richer context,\nhigher latency",
    xy=(4, graphrag_latency[-1]), xytext=(3.2, 3.5),
    fontsize=8, color=BLUE,
    arrowprops=dict(arrowstyle="->", color=BLUE, lw=1.2),
)
ax_lat.annotate(
    "VectorRAG: fast\nbut shallow retrieval",
    xy=(4, vectorrag_latency[-1]), xytext=(2.6, 0.2),
    fontsize=8, color=RED,
    arrowprops=dict(arrowstyle="->", color=RED, lw=1.2),
)

# ── Summary table as text box ────────────────────────────────────────────────
summary = (
    "GraphRAG avg (hops 1-4)\n"
    f"  Faithfulness : {np.mean(graphrag_faithfulness):.3f}\n"
    f"  Relevancy    : {np.mean(graphrag_relevancy):.3f}\n"
    f"  Precision    : {np.mean(graphrag_precision):.3f}\n"
    f"  Recall       : {np.mean(graphrag_recall):.3f}\n"
    "\nVectorRAG avg (hops 1-4)\n"
    f"  Faithfulness : {np.mean(vectorrag_faithfulness):.3f}\n"
    f"  Relevancy    : {np.mean(vectorrag_relevancy):.3f}\n"
    f"  Precision    : {np.mean(vectorrag_precision):.3f}\n"
    f"  Recall       : {np.mean(vectorrag_recall):.3f}"
)
ax_sum.axis("off")
ax_sum.text(
    0.5, 0.97, summary,
    transform=ax_sum.transAxes,
    fontsize=7.5, verticalalignment="top", horizontalalignment="center",
    bbox=dict(boxstyle="round,pad=0.5", facecolor="white",
              edgecolor="grey", alpha=0.85),
)

# ── Main title ───────────────────────────────────────────────────────────────
fig.suptitle(
    "GraphRAG vs VectorRAG — RAGAS Evaluation across Hop Depths 1–4",
    fontsize=14, fontweight="bold", y=1.01,
)

fig.savefig("ragas_metrics_plots.png", dpi=300, bbox_inches="tight")
print("Saved: ragas_metrics_plots.png")

