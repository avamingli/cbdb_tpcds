#!/usr/bin/env python3
"""Shared utilities for benchmark chart generation.

Provides automatic overlap detection and correction for matplotlib text
annotations.  Import from sibling scripts::

    from chart_utils import fix_text_overlaps
"""


def fix_text_overlaps(fig, ax, max_passes=5, pad_px=4):
    """Detect and fix overlapping text annotations on a matplotlib Axes.

    Call this after placing all text and before ``fig.savefig()``.  It
    renders the figure to compute text bounding boxes, then nudges
    overlapping texts apart vertically.

    Args:
        fig: matplotlib Figure
        ax: matplotlib Axes containing the text annotations
        max_passes: maximum adjustment iterations (default 5)
        pad_px: minimum pixel gap between labels (default 4)

    Returns:
        Number of adjustments made.
    """
    fig.canvas.draw()
    renderer = fig.canvas.get_renderer()

    # Only process free-floating text annotations (ax.texts),
    # not tick labels, axis labels, or title.
    texts = [t for t in ax.texts if t.get_visible()]
    if len(texts) < 2:
        return 0

    total_adj = 0
    for _ in range(max_passes):
        adj = 0
        bboxes = [t.get_window_extent(renderer) for t in texts]

        for i in range(len(texts)):
            for j in range(i + 1, len(texts)):
                bi, bj = bboxes[i], bboxes[j]
                # No overlap if separated in x or y (with padding)
                if (bi.x0 - pad_px >= bj.x1 + pad_px or
                    bi.x1 + pad_px <= bj.x0 - pad_px or
                    bi.y0 - pad_px >= bj.y1 + pad_px or
                    bi.y1 + pad_px <= bj.y0 - pad_px):
                    continue

                # Vertical overlap in display pixels
                overlap_h = min(bi.y1, bj.y1) - max(bi.y0, bj.y0) + pad_px
                if overlap_h <= 0:
                    continue

                # Convert pixel distance to data coordinates
                inv = ax.transData.inverted()
                _, d0 = inv.transform((0, 0))
                _, d1 = inv.transform((0, overlap_h))
                nudge = d1 - d0

                # Push the higher text upward
                xi, yi = texts[i].get_position()
                xj, yj = texts[j].get_position()
                if yi >= yj:
                    texts[i].set_position((xi, yi + nudge))
                else:
                    texts[j].set_position((xj, yj + nudge))
                adj += 1

        total_adj += adj
        if adj == 0:
            break
        fig.canvas.draw()
        renderer = fig.canvas.get_renderer()

    # Expand ylim if text was pushed above current limit
    if total_adj:
        _, ymax = ax.get_ylim()
        max_text_y = max(t.get_position()[1] for t in texts)
        if max_text_y > ymax * 0.92:
            ax.set_ylim(ax.get_ylim()[0], max_text_y * 1.12)

    return total_adj
