from defs import defs

lastUpdateDate = defs.dates.getLastUpdated()

with defs.NSE() as nse:
    while True:
        defs.dates.getNextDate()

        if defs.checkForHolidays(nse):
            continue

        # Validate NSE actions file
        defs.validateNseActionsFile(nse)

        # Download all files and validate for errors
        print('Downloading Files')

        # NSE bhav copy
        bhav_file = defs.downloadNseBhav(nse)

        try:
            print('Starting Data Sync')

            defs.updateSmeEOD(bhav_file)

            print('SME sync complete')
        except Exception as e:
            # rollback
            print(f"Error during data sync. {e!r}")
            defs.rollback(defs.daily_folder)

            defs.dates.dt = lastUpdateDate
            defs.dates.setLastUpdated()
            exit()

        # No errors continue

        # Adjust Splits and bonus
        print('Makings adjustments for splits and bonus')

        try:
            defs.adjustNseStocks()
        except Exception as e:
            print(
                f"Error while making adjustments. {e!r}\nAll adjustments have been discarded.")
            defs.rollback(defs.daily_folder)

            defs.dates.dt = lastUpdateDate
            defs.dates.setLastUpdated()
            exit()

        print('Cleaning up files')

        defs.cleanup((bhav_file,))

        defs.dates.setLastUpdated()
        lastUpdateDate = defs.dates.dt

        print(f'{defs.dates.dt:%d %b %Y}: Done\n{"-" * 52}')
