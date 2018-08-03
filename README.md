# Description

A deterministic code to generate a new DataFrame with exactly one row for each distinct "key" and the most common "value" for the corresponding key. A sample input with a valid output can be found below.



**Example Input:**

| **Key** | **Value**  |
|:----|:-------|
| A   | Blue   |
| A   | Red    |
| A   | Blue   |
| A   | Blue   |
| B   | Green  |
| B   | Purple |
| B   | Purple |
| B   | Green  |
| C   | Black  |

**Corresponding Output**

| **Key** | **Most Common**  |
|:----|:-------|
| A   | Blue   |
| B   | Green  |
| C   | Black  |
