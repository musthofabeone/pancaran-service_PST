module.exports = {
    base_url_SL : "https://10.10.16.21:50000",
    CompanyDB: "PST_TESTINGNEW",
    userName: "INT0003",
    Password: "H2H12345",
    sl_nextPort: 0,
    sl_realPort:0,

    base_url_xsjs:"https://10.10.16.21:4300",
    base_url_mobopay:"https://api.pancaran-group.co.id",
    base_url_payment:"https://centralnode.pancaran-group.co.id:8245",
    auth_basic: "U1lTVEVNOlBuQyRnUlBAMjAxOA==",

    max_retries:5,
    interval_retries:10000, // 5 menit
    timeOut:20000, // 1 menit
    resend_payment_retries:3,
    timedelay:20000,
    min_retries:0,
    interval_response_mobopay:20, // 20 menit
    inproses:false
}
