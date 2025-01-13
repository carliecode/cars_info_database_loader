def get_stg_jiji_cars_info_script():
    return """
        create or replace materialized view `applied-oxygen-284218.classified_data_scraping.stg_jiji_cars_info`
        as 
        with source_dataset as 
        (
        select
            json_extract(car_json_string, '$.AdvertPrice') as AdvertPrice,
            json_extract(car_json_string, '$.AdvertTitle') as AdvertTitle,
            json_extract(car_json_string, '$.DescriptionText') as DescriptionText,
            json_extract(car_json_string, '$.RegionText') as RegionText,
            json_extract(car_json_string, '$.ItemCondition')  as ItemCondition,
            json_extract(car_json_string, '$.FuelType')  as FuelType,
            json_extract(car_json_string, '$.VehicleTransmission') as VehicleTransmission,
            json_extract(car_json_string, '$.MileageFromOdometer') as MileageFromOdometer,
            json_extract(car_json_string, '$.PostedTimeDescription') as PostedTimeDescription,
            json_extract(car_json_string, '$.AdvertExtendedDescription') as AdvertExtendedDescription,
            json_extract(car_json_string, '$.SecondCondition') as SecondCondition,
            json_extract(car_json_string, '$.Make') as Make,
            json_extract(car_json_string, '$.Model') as Model,
            json_extract(car_json_string, '$.YearOfManufacture') as YearOfManufacture,
            json_extract(car_json_string, '$.Trim') as Trim,
            json_extract(car_json_string, '$.Body') as Body,
            json_extract(car_json_string, '$.Drivetrain') as Drivetrain,
            json_extract(car_json_string, '$.EngineSize') as EngineSize,
            json_extract(car_json_string, '$.NumberOfCylinders') as NumberOfCylinders,
            json_extract(car_json_string, '$.HorsePower') as HorsePower,
            json_extract(car_json_string, '$.Color') as Color,
            json_extract(car_json_string, '$.InteriorColor') as InteriorColor,
            json_extract(car_json_string, '$.Seats') as Seats,
            json_extract(car_json_string, '$.VinChassisNumber') as VinChassisNumber,
            json_extract(car_json_string, '$.RegisteredCar') as RegisteredCar,
            json_extract(car_json_string, '$.ExchangePossible') as ExchangePossible
        from `applied-oxygen-284218.classified_data_scraping.src_jiji_cars_info`
        ),
        cleaned_dataset as (
        select
            replace(cast(AdvertPrice as string), '"', '') as AdvertPrice,
            replace(cast(AdvertTitle as string), '"', '') as AdvertTitle,
            replace(cast(DescriptionText as string), '"', '') as DescriptionText,
            replace(cast(RegionText as string), '"', '') as RegionText,
            replace(cast(ItemCondition as string), '"', '') as ItemCondition,
            replace(cast(FuelType as string), '"', '') as FuelType,
            replace(cast(VehicleTransmission as string), '"', '') as VehicleTransmission,
            replace(cast(MileageFromOdometer as string), '"', '') as MileageFromOdometer,
            replace(cast(PostedTimeDescription as string), '"', '') as PostedTimeDescription,
            replace(cast(AdvertExtendedDescription as string), '"', '') as AdvertExtendedDescription,
            replace(cast(SecondCondition as string), '"', '') as SecondCondition,
            replace(cast(Make as string), '"', '') as Make,
            replace(cast(Model as string), '"', '') as Model,
            replace(cast(YearOfManufacture as string), '"', '') as YearOfManufacture,
            replace(cast(Trim as string), '"', '') as Trim,
            replace(cast(Body as string), '"', '') as Body,
            replace(cast(Drivetrain as string), '"', '') as Drivetrain,
            replace(cast(EngineSize as string), '"', '') as EngineSize,
            replace(cast(NumberOfCylinders as string), '"', '') as NumberOfCylinders,
            replace(cast(HorsePower as string), '"', '') as HorsePower,
            replace(cast(Color as string), '"', '') as Color,
            replace(cast(InteriorColor as string), '"', '') as InteriorColor,
            replace(cast(Seats as string), '"', '') as Seats,
            replace(cast(VinChassisNumber as string), '"', '') as VinChassisNumber,
            replace(cast(RegisteredCar as string), '"', '') as RegisteredCar,
            replace(cast(ExchangePossible as string), '"', '') as ExchangePossible
        from source_dataset
        )
        select 
        *
        from cleaned_dataset
    """
