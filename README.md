# To setup CCDW

1. Clone Existing Repository

    Using Tortoise GIT, right click in the folder where you want the CCDW folder created.
    Choose Git Clone... from the menu and enter the URL below into the URL field. Leave 
    all other defaults as they are.

    URL: https://github.com/haywood-ierg/ccdw-csc289

    The folder that is created will be referred as CCDW from now on.

2. In the *setup* folder, run *Create_Folders.bat*.

3. Copy the base data from the *basedata* folder into the data folder.

4. In the *import* folder, copy the *config_remote_template.yml* to *config.yml* and make the following changes:

    In the *sql* section, update the following items:

        * server: <Enter the server address>
        * db: CCDW
    
    In the *informer* section, update the following items:

        * export_path: <Full path to CCDW folder>/data
        * export_path_wStatus: <Full path to CCDW folder>/data
        * export_path_meta: <Full path to CCDW folder>/meta

