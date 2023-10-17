from orderManagement.Application.Interfaces.IOrderService import IOrderService
from orderManagement.Contracts.Common.Address import Address
from orderManagement.Contracts.Common.Product import Product
from orderManagement.Contracts.Common.TransitLocation import TransitLocation
from orderManagement.Contracts.Common.OrderLine import OrderLine
from orderManagement.Contracts.Common.Order import Order
from orderManagement.Contracts.Output.PlaceOrderResponse import PlaceOrderResponse
import orderManagement.Domain.Order


class OrderService(IOrderService):
    def __init__(self, publisher, cost_calculator_service, order_repository,
                 order_tracking_repository, product_availability_service):
        self._publisher = publisher
        self._costCalculatorService = cost_calculator_service
        self._orderRepository = order_repository
        self._orderTrackingRepository = order_tracking_repository
        self._productAvailabilityService = product_availability_service
        return

    def GetOrderHistory(self, customer_id):
        #  Load orders from the repository
        orders = self._orderRepository.Search(customer_id)
        # Convert to output contracts and return
        list_orders = []
        for order in orders:
            con_order = self.MapToContract(order, None)
            list_orders.append(con_order.asdict())
        return list_orders

    def GetOrderTrackingInfo(self, order_id):
        # Load order from the repository
        domain_order = self._orderRepository.Load(order_id)
        list_transit_location = []
        if domain_order is None:
            return list_transit_location
        # Load the transit locations
        domain_order.WithOrderTrackingRepository(self._orderTrackingRepository)
        domain_order.LoadTransitLocations()
        # //Convert to output contracts and return
        for transit in domain_order.TransitLocations:
            con_address = Address.Create(domain_order.BillingAddress.AddressLine1,
                                         domain_order.BillingAddress.AddressLine2,
                                         domain_order.BillingAddress.Country)
            con_transit_location = TransitLocation(transit.Name, transit.Date, con_address)
            list_transit_location.append(con_transit_location.asdict())
        return list_transit_location

    def PlaceOrder(self, request):
        # Create domain order from the request
        order_lines = []
        for order_line in request.Order.OrderLines:
            product = Product(order_line.Product.Stockcode, order_line.Product.ProductImageUrl,
                              order_line.Product.VolumetricWeight)
            order_line = OrderLine(product, order_line.Quantity, order_line.UnitPrice)
            order_lines.append(order_line)
        address1 = Address.Create(request.Order.BillingAddress.AddressLine1,
                                  request.Order.BillingAddress.AddressLine2,
                                  request.Order.BillingAddress.Country)
        address2 = Address.Create(request.Order.ShippingAddress.AddressLine1,
                                  request.Order.ShippingAddress.AddressLine2,
                                  request.Order.ShippingAddress.Country)
        domain_order = orderManagement.Domain.Order.Order.Create(order_lines, request.Order.CustomerId,
                                                                 address1, address2,
                                                                 request.Order.PromotionCode, request.Order.DatePlaced,
                                                                 self._costCalculatorService,
                                                                 self._productAvailabilityService,
                                                                 self._orderTrackingRepository)

        # Perform domain validation
        if domain_order.CanPlaceOrder(request.ExpectedTotalCost, request.ExpectedShippingCost):
            # store the order in the repository
            order_id = self._orderRepository.Store(domain_order)
            response = PlaceOrderResponse(True, "", order_id)
            # publish
            self._publisher.Publish(self.MapToContract(domain_order, order_id))
            return response.asdict()
        else:
            response = PlaceOrderResponse(False, "Order validation failed", None)
            return response.asdict()

    def MapToContract(self, order, order_id):

        order_lines = []
        for orderLine in order.OrderLines:
            order_lines.append(OrderLine(
                Product(orderLine.Product.Stockcode, orderLine.Product.ProductImageUrl,
                        orderLine.Product.VolumetricWeight),
                orderLine.Quantity,
                orderLine.UnitPrice))

        return Order.Create(order_id if (order_id is None) else order.OrderId,
                            order_lines, order.CustomerId, order.TotalCost, order.ShippingCost,
                            Address(order.BillingAddress.AddressLine1,
                                    order.BillingAddress.AddressLine2,
                                    order.BillingAddress.Country),
                            Address(order.ShippingAddress.AddressLine1,
                                    order.ShippingAddress.AddressLine2,
                                    order.ShippingAddress.Country),
                            order.PromotionCode, order.DatePlaced)
