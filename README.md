# datafusion-remote-table

## Goals
1. Execute SQL queries on remote databases and make results as datafusion table provider
2. Operator can be serialized for distributed execution
3. Record batches can be transformed before outputting to next operator