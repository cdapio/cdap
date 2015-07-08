require 'spec_helper'

describe 'cdap::security' do
  context 'using default cdap version' do
    let(:chef_run) do
      ChefSpec::SoloRunner.new(platform: 'centos', version: 6.6) do |node|
        node.automatic['domain'] = 'example.com'
        stub_command(/update-alternatives --display /).and_return(false)
      end.converge(described_recipe)
    end
    pkg = 'cdap-auth-server'

    %W(
      /etc/init.d/#{pkg}
    ).each do |file|
      it "creates #{file} from template" do
        expect(chef_run).to create_template(file)
      end
    end

    it 'installs cdap-security package' do
      expect(chef_run).to install_package('cdap-security')
    end

    it "creates #{pkg} service, but does not run it" do
      expect(chef_run).not_to start_service(pkg)
    end
  end
end
