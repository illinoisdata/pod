import math

COLORS = ["Acolor", "Bcolor", "Ccolor", "Dcolor", "Ecolor", "Fcolor", "Gcolor", "Hcolor"]
BAR_STYLE = r"""\pgfplotsset{
    barfig/.style={
        height=37mm,
        axis lines=left,
        ymode=linear,
        ymin=0.1,
        xmin = 0,
        xmax = 2,
        bar width=\subfigbarwidth,
        ybar,
        enlarge x limits=0,
        xtick = { 1 },
        xticklabels = { },
        xtick pos=left,
        xlabel shift=-4mm,
        ylabel=\textsf{Storage (GB)},
        ylabel near ticks,
        ylabel style={align=center},
        label style={font=\scriptsize\sf},
        legend style={
            at={(0.5, 1.05)},anchor=south,column sep=2pt,
            draw=black,fill=white,line width=.5pt,
            font=\tiny,
            /tikz/every even column/.append style={column sep=5pt}
        },
        legend columns=-1,
        colormap name=bright,
        every axis/.append style={font=\scriptsize},
        ymajorgrids,
        ylabel near ticks,
        legend image code/.code={%
            \draw[#1, draw=none] (0cm,-0.1cm) rectangle (0.4cm,0.1cm);
        },
    },
    barfig/.belongs to family=/pgfplots/scale,
}
"""


STACKED_BAR_STYLE = r"""\pgfplotsset{
    sbarfig/.style={
        height=37mm,
        axis lines=left,
        ymode=linear,
        ymin=0.1,
        xmin = 0,
        bar width=\subfigbarwidth,
        ybar stacked,
        enlarge x limits=0,
        xticklabel style={rotate=45},
        xtick pos=left,
        xlabel shift=-4mm,
        ylabel=\textsf{ Time Breakdown },
        ylabel near ticks,
        ylabel style={align=center},
        label style={font=\scriptsize\sf},
        legend style={
            at={(0.5, 1.05)},anchor=south,column sep=2pt,
            draw=black,fill=white,line width=.5pt,
            font=\tiny,
            /tikz/every even column/.append style={column sep=5pt}
        },
        legend columns=-1,
        colormap name=bright,
        every axis/.append style={font=\scriptsize},
        ymajorgrids,
        ylabel near ticks,
        legend image code/.code={%
            \draw[#1, draw=none] (0cm,-0.1cm) rectangle (0.4cm,0.1cm);
        },
    },
    sbarfig/.belongs to family=/pgfplots/scale,
}
"""


def get_header(num_figs, bar_width):
    width = round(1 / num_figs, 1)
    header = (
        r"""\begin{figure*}[t]
\def\subfigwidth{"""
        + str(width)
        + r"""\linewidth}
\def\subfigbarwidth{"""
        + str(bar_width)
        + r"""}

"""
    )
    return header


SUBFIG_HEADER = r"""\begin{subfigure}[t]{\subfigwidth} \centering
\begin{tikzpicture}
"""


def create_bar_axis(labels, data):
    axis_str = (
        r"""
\begin{axis}[
    barfig,
    width="""
        + str(len(labels) * 10)
        + r"""mm,
    ymax="""
        + str(max(1, round(max(data)) + 10))
        + r""",
    ytick = {"""
        + ",".join([str((s)) for s in list(range(0, (round(max(data)) + 20), 10))])
        + r"""},
]

"""
    )
    return axis_str


def create_time_table(data, name):
    table = r"""\pgfplotstableread{idx  time_val
"""
    for i, val in enumerate(data):
        row_str = f"{2*(i+1)}   {val}\n"
        table += row_str
    table += r"""}\TBL""" + name + "\n"
    return table


def create_bar_plots(labels, name):
    colors = ["Acolor", "Bcolor", "Ccolor", "Dcolor", "Ecolor", "Fcolor", "Gcolor", "Hcolor"]
    plot_str = r""
    for i in range(1, len(labels)):
        plot_str += (
            r"""\addplot plot [
    fill="""
            + colors[i - 1]
            + r""",draw=none]
table[x=idx, y="""
            + labels[i]
            + r"""] {\TBL"""
            + name
            + "};"
        )
        plot_str += "\n"
    return plot_str


def create_stacked_bar_plots(expname):
    plot_str = r""
    plot_str += (
        r"""\addplot plot [ fill=""" + COLORS[0] + r""",draw=none] table[x=idx, y=time_val] {\TBL""" + expname + "Lock};\n"
    )
    plot_str += (
        r"""\addplot plot [ fill=""" + COLORS[1] + r""",draw=none] table[x=idx, y=time_val] {\TBL""" + expname + "Save};\n"
    )
    return plot_str


def create_table_vals(data):
    vals = ["1"]
    for val in data:
        vals.append(str(val))
    return "    ".join(vals)


def create_stacked_bar_axis(lock_times, save_times, labels):
    bar_height = [lock_times[i] + save_times[i] for i in range(len(lock_times))]
    max_height = max(bar_height)
    axis_str = (
        (
            r"""
\begin{axis}[
    sbarfig,
    width=\linewidth,
    xmax="""
            + str(2 * len(labels) + 3)
            + r""",
    xtick={"""
            + ", ".join([str(2 * n) for n in range(len(labels))])
        )
        + r"""},
    xticklabels={"""
        + ", ".join(labels)
        + r"""},
    ymax="""
        + str(max(1, 100 * int(max_height // 100)))
        + r""",
    ytick = {"""
        + r",".join(
            [str(s) for s in list(range(0, (100 * int(max_height // 100) + 100), int(10 * math.ceil(max_height / 100))))]
        )
        + r"""},
]

"""
    )
    return axis_str


def create_line_table(data, name):
    tables = ""
    names = []
    for sut, times in data.items():
        tables += r"""\pgfplotstableread{idx  time_val
"""
        table_name = r"\TBL" + name + sut
        for time_data in times:
            tid, avg_time = time_data
            tables += f"{tid}   {avg_time}\n"
        tables += r"}" + table_name + "\n\n"
        names.append(table_name)
    # print(tables)
    return tables + "\n\n", names


def create_line_axis(data):
    max_x = 0
    max_y = 0
    for sut, times in data.items():
        max_x = max(max_x, times[-1][0])
        max_y = max(max_y, max(times, key=lambda x: x[1])[1])

    axis_str = (
        r"""
\begin{axis}[
    xmax="""
        + str(max_x)
        + r""",
    ymax="""
        + str(max(1, round(max_y)))
        + r""",
    width=66mm,
]

"""
    )
    return axis_str


def create_line_plots(table_names):

    plot_str = r""
    for i, name in enumerate(table_names):
        plot_str += r"\addplot[mark=*," + COLORS[i] + r"] plot table[x=idx, y=time_val] {" + name + "};\n\n"
    # print(plot_str)
    return plot_str


def get_legend(labels):
    legend = r"""
\begin{subfigure}[b]{\linewidth} \centering
\begin{tikzpicture}
    \begin{axis}[
            width=\linewidth,
            hide axis,
            xmin=10,
            xmax=50,
            ymin=0,
            ymax=0.4,
            legend style={
                at={(0.5,0)}, % Adjust the placement if necessary
                anchor=south,
                draw=black, fill=white, line width=.5pt,
                font=\scriptsize,
            },
            legend columns=4,
        ]

        """
    for i in range(len(labels)):
        legend += (
            r"""\addlegendimage{"""
            + COLORS[i]
            + r"""}
        \addlegendentry{"""
            + labels[i]
            + r"""};
        
        """
        )

    legend += r"""    \end{axis}
\end{tikzpicture}
\end{subfigure}
"""
    return legend
