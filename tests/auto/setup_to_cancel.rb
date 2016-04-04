require 'date'
require 'net/http'
require 'json'
require_relative 'rest_api'

$B2B_API_URI = ENV['PUBLIC_API_URL'] || 'http://localhost:5001'
$B2B_PROCESSOR_URI = ENV['AUTOMATIC_PROCESS_URL'] || 'http://localhost:5002'
$BANKRUPTCY_REGISTRATION_URI = ENV['LAND_CHARGES_URL'] || 'http://localhost:5004'
$LAND_CHARGES_URI = ENV['LAND_CHARGES_URL'] || 'http://localhost:5004'
$CASEWORK_API_URI = ENV['CASEWORK_API_URL'] || 'http://localhost:5006'
$LEGACY_DB_URI = ENV['LEGACY_ADAPTER_URL'] || 'http://localhost:5007'
$FRONTEND_URI = ENV['CASEWORK_FRONTEND_URL'] || 'http://localhost:5010'

def create_reg_document(date, number)
    uri = URI($CASEWORK_API_URI)
    http = Net::HTTP.new(uri.host, uri.port)
    request = Net::HTTP::Post.new("/forms/A4?type=K1")
    folder = File.dirname(__FILE__)
    request["Content-Type"] = "image/tiff"
    image = "#{folder}/img.tiff" 
    request.body = IO.binread(image)
    response = http.request(request)
    docid = JSON.parse(response.body)['id']
    cw_api = RestAPI.new($CASEWORK_API_URI)
    puts "/registered_forms/#{date}/#{number}"
    puts cw_api.put("/registered_forms/#{date}/#{number}", JSON.generate({"id" => docid}))
    docid
end

`ruby /vagrant/apps/legacy-adapter/data/clear.rb`
`reset-data`

lc_private = '{"parties": [{"type": "Estate Owner","names":[{
"type": "Private Individual","private": {"forenames": ["Bob", "Oscar", "Francis"],"surname": "Howard"}
}]}],
"class_of_charge": "C1",	
"particulars": {"counties": ["Devon", "Somerset"],"district": "South Hams","description": "The House At The Beach"},
"applicant": {"name": "Some Court","address": "11 Court Road, Court Town","key_number": "7654321",
"reference": "ARGL1234567", "address_type": "RM"},"additional_information": ""}'


lc_api = RestAPI.new($LAND_CHARGES_URI)
reg1 = lc_api.post('/registrations?dev_date=2014-01-01', lc_private)
puts reg1

reg1['new_registrations'].each do |can|
    create_reg_document(can['date'], can['number'])
end

puts `synchronise 2014-01-01 2>&1`

cancel_1002 = '{"applicant": {"address_type":"RM", "name": "S & H Legal Group", "reference": "sddsds", "address": "49 Camille Circles\r\nPort Eulah\r\nPP39 6BY", "key_number": "1234567"}, "document_id": 31, "registration": {"date": "2014-01-01"}, "registration_no": "1007", "update_registration": {"type": "Cancellation"}}'

lc_api = RestAPI.new($LAND_CHARGES_URI)
reg1 = lc_api.post('/cancellations', cancel_1002)
puts reg1
reg1['cancellations'].each do |can|
    create_reg_document(can['date'], can['number'])
end

puts `synchronise 2016-04-04 2>&1`
#puts "synchronise 2016-04-01"