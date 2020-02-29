# dbds-project-1

<b>Configuration</b>

* Always start off by creating a local_config.json file. This file should <b>NOT</b> be checked into version control.
It will contain AWS secrets that I will give you and failing to protect those will cost us money.

* A sample format for local_config.json can be found in the local_config_template.json. Replicate that file and replace
the variables accordingly.

<b>Description of the project:</b>

* The feed_data.py file feeds the instacart data on AWS S3 to RDS MySql. It should ideally only be run once unless the
data changes.
