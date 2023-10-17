import datetime
from orderManagement.Domain.Address import Address
from orderManagement.Domain.Interfaces.IOrderTrackingRepository import IOrderTrackingRepository
from orderManagement.Domain.TransitLocation import TransitLocation


class OrderTrackingRepository(IOrderTrackingRepository):
    def GetTransitLocations(self, order_id):
        address1 = Address.Create("addressLine1", "addressLine2", "country")
        address2 = Address.Create("addressLine1", "addressLine2", "country")
        transit_location1 = TransitLocation("loc1", datetime.datetime.now(), address1)
        transit_location2 = TransitLocation("loc2", datetime.datetime.now(), address2)
        transit_locations = [transit_location1, transit_location2]
        return transit_locations

