# OMERO Super-Resolution Tables

Example of storing super-resolution data in OMERO.

1. Edit [input.yml](input.yml) with your OMERO connection details (`host`, `user`, `password`), and the Object type and ID that the table should be attached to (`parent`).

2. Run:

        python sr_to_omero_table.py input.yml
