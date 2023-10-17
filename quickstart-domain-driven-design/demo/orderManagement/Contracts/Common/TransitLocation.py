class TransitLocation:
    def __init__(self, name, date, address):
        self.Name = name
        self.mDate = date
        self.Address = address

    @staticmethod
    def Create(name, date, address):
        return TransitLocation(name, date, address)

    # for making JSON serializable, convert to dict
    def asdict(self):
        return {'Name': self.Name, 'Date': self.mDate, 'Address': self.Address.asdict()}


