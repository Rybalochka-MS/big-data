import numpy as np
import matplotlib.pyplot as plt
from matplotlib.cm import get_cmap


def compress_dict(base_dict):
    """
    Makes a dictionary more appropriate for processing

    Input:
        base_dict - a dictionary in a formed returned by query.
    """
    for year, dicts in base_dict.items():
        dict_for_year = {}
        for item in dicts:
            vals = list(item.values())
            dict_for_year[vals[0]] = int(vals[1])
        base_dict[year] = {key: dict_for_year[key] for key in sorted(dict_for_year.keys())}


def bar_builer(a, year):
    """Input:
            a: dictionary of pairs entry-quantity
       Output:
            fig, axes - barplot of quantities per entry"""
    fig, axes = plt.subplots(1)
    colors = get_cmap("tab20").colors
    axes.bar(a.keys(), a.values(), color=colors)

    ax_title = str(year)
    axes.set_title(ax_title)
    axes.set_xlabel('Entries')
    axes.set_ylabel('Quantity')
    fig.autofmt_xdate(rotation=45)

    axes.tick_params(axis='x', labelsize='small')
    return fig, axes


def pie_builer(a, year):
    """Input:
            a: dictionary of pairs entry-quantity
       Output:
            fig, axes - pie chart of quantities per entry"""
    fig, axes = plt.subplots(1)
    colors = get_cmap("tab20").colors + get_cmap("tab20b").colors
    wedges, texts, autotexts = axes.pie(a.values(), colors=colors, autopct='%1.1f%%', textprops={'fontsize': 6})
    labels = list(a.keys())

    bbox_props = dict(boxstyle="square,pad=0.3", fc="w", ec="k", lw=0.72)
    kw = dict(arrowprops=dict(arrowstyle="-"),
              bbox=bbox_props, zorder=0, va="center")

    for i, p in enumerate(wedges):
        ang = (p.theta2 - p.theta1) / 2. + p.theta1
        y = np.sin(np.deg2rad(ang))
        x = np.cos(np.deg2rad(ang))
        horizontalalignment = {-1: "right", 1: "left"}[int(np.sign(x))]
        connectionstyle = "angle,angleA=0,angleB={}".format(ang)
        kw["arrowprops"].update({"connectionstyle": connectionstyle})
        axes.annotate(labels[i], xy=(x, y), xytext=(1.35 * np.sign(x), 1.4 * y),
                      horizontalalignment=horizontalalignment, fontsize=8, **kw)

    ax_title = str(year) + " (" + str(sum(a.values())) + " cars total)"
    axes.set_title(ax_title, y=1.08)
    return fig, axes
