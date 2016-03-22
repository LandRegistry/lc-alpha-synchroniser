require 'date'
require 'net/http'
require 'json'
require_relative 'rest_api'


# Register some INS bankruptcy submissions
# Fake register some paper applications
# Register amendments too

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



$B2B_API_URI = ENV['PUBLIC_API_URL'] || 'http://localhost:5001'
$B2B_PROCESSOR_URI = ENV['AUTOMATIC_PROCESS_URL'] || 'http://localhost:5002'
$BANKRUPTCY_REGISTRATION_URI = ENV['LAND_CHARGES_URL'] || 'http://localhost:5004'
$LAND_CHARGES_URI = ENV['LAND_CHARGES_URL'] || 'http://localhost:5004'
$CASEWORK_API_URI = ENV['CASEWORK_API_URL'] || 'http://localhost:5006'
$LEGACY_DB_URI = ENV['LEGACY_ADAPTER_URL'] || 'http://localhost:5007'
$FRONTEND_URI = ENV['CASEWORK_FRONTEND_URL'] || 'http://localhost:5010'

b2b_one_name_valid = '{"key_number":"1234567","application_ref":"APP01","application_type":"PA(B)","application_date":"2016-01-01","debtor_names":[{"forenames":["Bob","Oscar","Francis"],"surname":"Howard"}],"gender":"Unknown","occupation":"Civil Servant","trading_name":"","residence":[{"address_lines":["1 The Street","The Town"],"postcode":"AA1 1AA","county":"The County"}],"residence_withheld":false,"date_of_birth":"1980-01-01"}'

#b2b_one_name_no_residence = '{"key_number":"1234567","application_ref":"APP01","application_type":"PA(B)","application_date":"2016-01-01","debtor_names":[{"forenames":["Bob","Oscar","Francis"],"surname":"Howard"}],"gender":"Unknown","occupation":"Civil Servant","trading_name":"","residence_withheld":true,"date_of_birth":"1980-01-01"}'

b2b_two_name_valid = '{"key_number":"1234567","application_ref":"APP01","application_type":"PA(B)","application_date":"2016-01-01","debtor_names":[{"forenames":["Bob","Oscar","Francis"],"surname":"Howard"},{"forenames":["Robart"],"surname":"Howard"}],"gender":"Unknown","occupation":"Civil Servant","trading_name":"","residence":[{"address_lines":["1 The Street","The Town"],"postcode":"AA1 1AA","county":"The County"}],"residence_withheld":false,"date_of_birth":"1980-01-01"}'

`reset-data`

public_api = RestAPI.new($B2B_API_URI)
public_api.post("/bankruptcies", b2b_one_name_valid)
#public_api.post("/bankruptcies", b2b_one_name_no_residence)
#public_api.post("/bankruptcies", b2b_two_name_valid)

lc_private = '{"parties": [{"type": "Estate Owner","names":[{
"type": "Private Individual","private": {"forenames": ["Bob", "Oscar", "Francis"],"surname": "Howard"}
}]}],
"class_of_charge": "C1",	
"particulars": {"counties": ["Devon"],"district": "South Hams","description": "The House At The Beach"},
"applicant": {"name": "Some Court","address": "11 Court Road, Court Town","key_number": "7654321",
"reference": "ARGL1234567", "address_type": "RM"},"additional_information": ""}'

lc_private2 = '{"parties": [{"type": "Estate Owner","names":[{
"type": "Private Individual","private": {"forenames": ["Sam", "William"],"surname": "Smith"}
}]}],
"class_of_charge": "C2",	
"particulars": {"counties": ["Devon"],"district": "South Hams","description": "The House At The Beach"},
"applicant": {"name": "Some Court","address": "11 Court Road, Court Town","key_number": "7654321",
"reference": "ARGL1234567", "address_type": "RM"},"additional_information": ""}'



lc_api = RestAPI.new($LAND_CHARGES_URI)
reg1 = lc_api.post('/registrations?dev_date=2010-04-01', lc_private)
reg2 = lc_api.post('/registrations?dev_date=2010-04-01', lc_private2)

reg1_no = reg1['new_registrations'][0]['number']
reg1_date = reg1['new_registrations'][0]['date']
reg2_no = reg2['new_registrations'][0]['number']
reg2_date = reg2['new_registrations'][0]['date']
#puts reg1


docid1 = create_reg_document(reg1_date, reg1_no)
docid2 = create_reg_document(reg2_date, reg2_no)

puts "==================================================="
puts "  SYNC REGISTRATIONS"
puts `synchronise 2010-04-01 2>&1`
puts "---------------------------------------------------"

#"update_registration"/'Rectification', 'Correction', 'Amendment'

lc_private_v2 = '{"update_registration": {"type": "Rectification"}, "parties": [{"type": "Estate Owner","names":[{
"type": "Private Individual","private": {"forenames": ["Robert", "Oscar", "Francis"],"surname": "Howard"}
}]}],
"class_of_charge": "C1",	
"particulars": {"counties": ["Devon"],"district": "South Hams","description": "The House At The Beach"},
"applicant": {"name": "Some Court","address": "11 Court Road, Court Town","key_number": "7654321", "address_type": "RM",
"reference": "ARGL1234567"},"additional_information": ""}'

lc_private2_v2 = '{"update_registration": {"type": "Rectification"}, "parties": [{"type": "Estate Owner","names":[{
"type": "Private Individual","private": {"forenames": ["Sam", "William"],"surname": "Smith"}
}]}],
"class_of_charge": "C2",	
"particulars": {"counties": ["Devon"],"district": "South Hams","description": "The House Under the Water"},
"applicant": {"name": "Some Court","address": "11 Court Road, Court Town","key_number": "7654321", "address_type": "RM",
"reference": "ARGL1234567"},"additional_information": ""}'

puts "PUT Rectifications"
reg1_2 = lc_api.put("/registrations/#{reg1_date}/#{reg1_no}", lc_private_v2) # Type 2 amendment
reg2_2 = lc_api.put("/registrations/#{reg2_date}/#{reg2_no}", lc_private2_v2) # Type 2 amendment
#{"request_id"=>13127, "new_registrations"=>[{"number"=>1000, "county"=>"Devon", "date"=>"2010-04-01"}]}

puts reg1_2
puts reg1_2['new_registrations']
puts reg1_2['new_registrations'][0]
puts reg1_2['new_registrations'][0]['number']

reg1_no = reg1_2['new_registrations'][0]['number']
reg1_date = reg1_2['new_registrations'][0]['date']
reg2_no = reg2_2['new_registrations'][0]['number']
reg2_date = reg2_2['new_registrations'][0]['date']

puts "Fake documents"
docid1 = create_reg_document(reg1_date, reg1_no)
docid2 = create_reg_document(reg2_date, reg2_no)


puts "==================================================="
puts "  SYNC RECTIFICATIONS"
puts `synchronise 2>&1`
puts "---------------------------------------------------"