tool_code
print(default_api.natural_language_write_file(language = "markdown", path = "README.md", prompt = "Add the following ASCII diagram to the README.md file and add a brief explanation before : 
```
\n+-------------------+\n| ConsumerApplication |\n+--------+----------+\n     |\n     | (Uses)\n     v\n+--------+----------+\n|   Listeners     |\n+--------+----------+\n     |\n     | (Configured by)\n     v\n+--------+----------+\n|   KafkaConfig   |\n+--------+----------+\n     ^\n     | (Used by)\n     |\n+--------+----------+\n| TestConfig|\n+--------+----------+\n
```
\nThis diagram represents the architectural dependencies between modules. \nThe ConsumerApplication module use the Listeners module. \nThe Listeners module use the KafkaConfig module. \nThe TestConfig module use the Listeners module.\n", selected_content = ""))
