require '..\lib\schema'

describe Schema, "#initialize" do

	before(:each) do
	  @schema = Schema.new('cube', {}, {})
	end

	it "names with whitespaces should throw exception" do
	  expect {Schema.new('Big Whitespace',{},{})}.to raise_error
	end

	it "should have correct id" do
    	@schema.id.should eql('cube_id')
    end

	it "should have correct attributes definition" 

	it "should have correct metrics definition" 

end