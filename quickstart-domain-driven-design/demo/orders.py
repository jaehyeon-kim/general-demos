from orderManagement.Contracts.Input.PlaceOrderRequest import PlaceOrderRequest
from orderManagement.Contracts.Common.Address import Address
from orderManagement.Contracts.Common.Product import Product
from orderManagement.Contracts.Common.OrderLine import OrderLine
from orderManagement.Contracts.Common.Order import Order
from orderManagement.Application.OrderService import OrderService
from connexion import NoContent
from flask import request
from flask_injector import inject


def json2Address(json_dic):
    addressLine1 = json_dic["AddressLine1"]
    addressLine2 = json_dic["AddressLine2"]
    country = json_dic["Country"]
    return Address(addressLine1, addressLine2, country)


def json2Product(json_dic):
    stockcode = json_dic["Stockcode"]
    productImageUrl = json_dic["ProductImageUrl"]
    volumetricWeight = json_dic["VolumetricWeight"]
    return Product(stockcode, productImageUrl, volumetricWeight)


def json2TransitLocation(json_dic):
    name = json_dic["Name"]
    date = json_dic["Date"]
    address = json2Address(json_dic["Address"])
    return Address(name, date, address)


def json2TransitLocations(json_dic):
    transitLocations = []
    for value in json_dic:
        transitLocation = json2TransitLocation(value)
        transitLocations.append(transitLocation)
    return transitLocations


def json2OrderLine(json_dic):
    product = json2Product(json_dic["Product"])
    quantity = json_dic["Quantity"]
    unitPrice = json_dic["UnitPrice"]
    return OrderLine(product, quantity, unitPrice)


def json2OrderLines(json_dic):
    orderlines = []
    for value in json_dic:
        orderLine = json2OrderLine(value)
        orderlines.append(orderLine)
    return orderlines


def json2Order(json_dic):
    orderId = json_dic["OrderId"]
    orderLines = json2OrderLines(json_dic["OrderLines"])
    customerId = json_dic["CustomerId"]
    totalCost = json_dic["TotalCost"]
    shippingCost = json_dic["ShippingCost"]
    billingAddress = json2Address(json_dic["BillingAddress"])
    shippingAddress = json2Address(json_dic["ShippingAddress"])
    promotionCode = json_dic["PromotionCode"]
    datePlaced = json_dic["DatePlaced"]
    transitLocations = json2TransitLocations(json_dic["TransitLocations"])

    order = Order(orderId, orderLines, customerId, totalCost, shippingCost, billingAddress, shippingAddress,
                  promotionCode, datePlaced, transitLocations)
    return order


def joson2placeOrderRequest(json_dic):
    expectedTotalCost = json_dic["ExpectedTotalCost"]
    expectedShippingCost = json_dic["ExpectedShippingCost"]
    order = json2Order(json_dic["Order"])
    placeOrderRequest = PlaceOrderRequest(order, expectedTotalCost, expectedShippingCost)
    return placeOrderRequest


@inject
def post(order_service: OrderService):
    req_json = request.get_json()
    placeOrderRequest = joson2placeOrderRequest(req_json)
    placeOrderResponse = order_service.PlaceOrder(placeOrderRequest)
    return placeOrderResponse


@inject
def getOrders(CustomerId, order_service: OrderService):
    id = int(CustomerId)
    orders = order_service.GetOrderHistory(id)
    if orders is None:
        return NoContent, 404
    return orders


@inject
def getTransitLocations(orderId, order_service: OrderService):
    id = int(orderId)
    transitLocations = order_service.GetOrderTrackingInfo(id)
    if transitLocations is None:
        return NoContent, 404

    return transitLocations
