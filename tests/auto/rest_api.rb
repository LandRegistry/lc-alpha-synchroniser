class RestAPI
    attr_reader :response, :data
    @@last_response = nil

    def self.last_response
        @@last_response
    end

    def initialize(uri)
        @uri = URI(uri)
        @http = Net::HTTP.new(@uri.host, @uri.port)
    end

    def postXML(url, data)
        request = Net::HTTP::Post.new(url)
        request.body = data
        request["Content-Type"] = "text/xml"
        @response = @http.request(request)
        @@last_response = @response
        if @response.body == ""
            nil
        else
            @data = JSON.parse(@response.body)
        end
    end

    def post(url, data = nil)
        request = Net::HTTP::Post.new(url)
        unless data.nil?
            request.body = data
            request["Content-Type"] = "application/json"
        end
        @response = @http.request(request)
        @@last_response = @response
        if @response.body == ""
            nil
        else
            @data = JSON.parse(@response.body)
        end
    end

    def get(url, data = nil)
        request = Net::HTTP::Get.new(url)
        unless data.nil?
            request.body = data
            request["Content-Type"] = "application/json"
        end
        @response = @http.request(request)
        @@last_response = @response
        @data = JSON.parse(@response.body)
    end
    
    def getbin(url, data = nil)
        request = Net::HTTP::Get.new(url)
        unless data.nil?
            request.body = data
            request["Content-Type"] = "application/json"
        end
        @response = @http.request(request)
        @@last_response = @response
        @data = @response.body
    end

    def put(url, data)
        request = Net::HTTP::Put.new(url)
        request.body = data
        request["Content-Type"] = "application/json"
        @response = @http.request(request)
        @@last_response = @response
        if @response.body == ""
            nil
        else
            @data = JSON.parse(@response.body)
        end
    end
end
