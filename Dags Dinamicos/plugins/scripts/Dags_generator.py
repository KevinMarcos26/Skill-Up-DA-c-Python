from jinja2 import Environment, FileSystemLoader
import yaml
import os

file_dir = os.path.dirname(os.path.abspath(f"{__file__}/../../"))
plugins_dir = f'{file_dir}/plugins'

env = Environment(loader=FileSystemLoader(plugins_dir))
template = env.get_template('templates/GyH_template.jinja2')

for filename in os.listdir(f'{plugins_dir}/config'):
    if filename.endswith('.yaml'):
        with open(f'{plugins_dir}/config/{filename}') as configfile:
            config = yaml.safe_load(configfile)
            with open(f"{file_dir}/dags/{config['university']}_dag_etl.py", "w") as f:
                f.write(template.render(config))
