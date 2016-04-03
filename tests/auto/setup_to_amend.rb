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


wob_registration = '{"class_of_charge": "WOB", "parties": [{"occupation": "Who Knows", "residence_withheld": false,
"legal_body_ref_year": "2012", "names": [{"type": "Private Individual", "private": {"forenames": ["Virginie", "May"], 
"surname": "Conn"}}, {"type": "Private Individual", "private": {"forenames": ["Virginia"], "surname": "Conn"}}], 
"type": "Debtor", "addresses": [{"address_string": "1 Not Specified Blah PL1 1AA", "type": "Residence",
"address_lines": ["1 Not Specified"], "postcode": "PL1 1AA", "county": "Blah"}], "case_reference": "Swindon 188 of 2012",
"legal_body": "Swindon", "legal_body_ref_no": "188", "trading_name": " "}], "applicant": {"name": "S & H Legal Group", "reference": " ", "address": "49 Camille Circles Port Eulah PP39 6BY", "key_number": "1234567", "address_type": "RM"}}'

lc_api = RestAPI.new($LAND_CHARGES_URI)
reg1 = lc_api.post('/registrations?dev_date=2016-01-01', wob_registration)
puts reg1
reg1['new_registrations'].each do |can|
    puts can['date']
    puts can['number']
    create_reg_document(can['date'], can['number'])
end
date = '2016-01-01'
number = reg1['new_registrations'][0]['number']

#puts `synchronise 2016-01-01 2>&1`

wob_amend = '{"class_of_charge": "WOB", "parties": [{"occupation": "Who Knows", "type": "Debtor", "legal_body_ref_year": "2012", "names": [{"type": "Private Individual", "private": {"forenames": ["Virginie", "May"], "surname": "Connblha"}}, {"type": "Private Individual", "private": {"forenames": ["Virginia"], "surname": "Conn"}}, {"type": "Private Individual", "private": {"forenames": ["Virginia"], "surname": "Smith"}}], "addresses": [{"postcode": "PL1 1AA", "type": "Residence", "address_string": "1 Not Specified Blah PL1 1AA", "address_lines": ["1 Not Specified"], "county": "Blah"}], "case_reference": "Swindon 188 of 2012", "legal_body": "Swindon", "legal_body_ref_no": "188", "trading_name": " ", "residence_withheld": false}], "update_registration": {"type": "Amendment"}, "applicant": {"name": "S & H Legal Group", "reference": " ", "address": "49 Camille Circles Port Eulah PP39 6BY", "key_number": "1234567", "address_type": "RM"}}'


reg1 = lc_api.put("/registrations/#{date}/#{number}", wob_amend)
puts reg1

reg1['new_registrations'].each do |can|
    puts can['date']
    puts can['number']
    create_reg_document(can['date'], can['number'])
end

#puts `synchronise 2016-04-03 2>&1`
