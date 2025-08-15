def format_results(results):
    """Formats BigQuery results into a human-readable string."""
    rows = list(results)
    if not rows:
        return "No results found."

    if len(rows[0].keys()) > 5:
        # Format as a table for wide results
        header = " | ".join(rows[0].keys())
        separator = "-" * len(header)
        body = "\n".join([" | ".join(map(str, row.values())) for row in rows])
        return f"<pre>\n{header}\n{separator}\n{body}\n</pre>"
    else:
        # Format as a bulleted list for narrow results
        output = ""
        for row in rows:
            output += "* "
            for key, value in row.items():
                output += f"{key}: {value}, "
            output = output.strip(", ") + "\n"
        return output
