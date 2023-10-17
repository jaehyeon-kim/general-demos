import logging
import connexion
from flask_injector import FlaskInjector
from injector import Binder
from orderManagement.Application.OrderService import OrderService
from orderManagement.Infrastructure.Publisher.Publisher import Publisher
from orderManagement.Domain.Services.CostCalculatorService import CostCalculatorService
from orderManagement.Infrastructure.Repository.OrderRepository import OrderRepository
from orderManagement.Infrastructure.Repository.OrderTrackingRepository import OrderTrackingRepository
from orderManagement.Infrastructure.Services.ProductAvailabilityService import ProductAvailabilityService


logging.basicConfig(level=logging.INFO)


def configure(binder: Binder) -> Binder:
    binder.bind(interface=OrderService,
                to=OrderService(Publisher(), CostCalculatorService(), OrderRepository(),
                                OrderTrackingRepository(), ProductAvailabilityService()))
    return binder


if __name__ == '__main__':
    app = connexion.FlaskApp(__name__, port=9000)#, specification_dir='./')
    app.add_api('order-api.yaml', arguments={'title': 'eCommerce System Example'})
    FlaskInjector(app=app.app, modules=[configure])
    app.run(host="127.0.0.1")





