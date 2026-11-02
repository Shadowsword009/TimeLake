
import yaml
from src.timelake.generator import generate_dataset


def main():
    with open("config/settings.yaml", "r") as file:
        settings = yaml.safe_load(file)

    generator_settings = settings["generator"]
    data_settings = settings["data"]

    generate_dataset(
        num_records=generator_settings["default_records"],
        customer_count=generator_settings["customer_count"],
        start_date=generator_settings["start_date"],
        end_date=generator_settings["end_date"],
        seed=generator_settings["random_seed"],
        output_dir=data_settings["input_dir"]
    )


if __name__ == "__main__":
    main()
