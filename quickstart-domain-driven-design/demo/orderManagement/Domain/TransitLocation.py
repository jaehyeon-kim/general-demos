class TransitLocation:
    def __init__(self, name, date, address):
        self.Name = name
        self.Date = date
        self.Address = address

    @staticmethod
    def Create(name, date, address):
        return TransitLocation(name, date, address)


