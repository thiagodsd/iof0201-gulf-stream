#!/bin/bash

# Generate a timestamp
timestamp=$(date +%Y%m%d%H%M%S)

# File names with timestamp
tex_file="equation_${timestamp}.tex"
pdf_file="equation_${timestamp}.pdf"
png_file="equation_${timestamp}.png"

# Step 1: Write LaTeX commands into a file using a quoted EOF to prevent variable expansion
cat << 'EOF' > "$tex_file"
\documentclass[12pt]{standalone}
\begin{document}
$\rho(S) = \rho_0 + \beta (S - S_0)$
\end{document}
EOF

# Step 2: Compile the LaTeX file to PDF
pdflatex -jobname=equation_${timestamp} "\input{$tex_file}"

# Step 3: Convert the PDF to PNG with higher resolution
gs -sDEVICE=pngalpha -o "$png_file" -r600 "$pdf_file"
