﻿using Microsoft.AspNetCore.Mvc;
using Newtonsoft.Json;
using RabbitMQ.Client;
using RABBITMQ_TUTORIAL.Models;
using RABBITMQ_TUTORIAL.Service;
using System.Diagnostics;
using System.Text;

namespace RABBITMQ_TUTORIAL.Controllers
{
    public class HomeController : Controller
    {
        private readonly ILogger<HomeController> _logger;
        private readonly IRabbitMQService _rabbitMQService;

        public HomeController(ILogger<HomeController> logger, IRabbitMQService rabbitMQService)
        {
            _logger = logger;
            _rabbitMQService = rabbitMQService;
        }

        public IActionResult Index()
        {

            for (int i = 0; i < 10; i++)
            {
                _rabbitMQService.PublishMessage("Kuyruk37", i+". Genel mesaj başlıksız");
                _rabbitMQService.ProduceSpecialMessageWithUserId(37,"Kuyruk37", "37 idli kullanıcı mesajı");
                _rabbitMQService.ProduceSpecialMessageWithUserId(45, "Kuyruk37", "45 idli kullanıcı mesajı");

                Thread.Sleep(3000);
            }

            return View();
        }
       
       

        [ResponseCache(Duration = 0, Location = ResponseCacheLocation.None, NoStore = true)]
        public IActionResult Error()
        {
            return View(new ErrorViewModel { RequestId = Activity.Current?.Id ?? HttpContext.TraceIdentifier });
        }
    }
}