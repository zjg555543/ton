#include "dht.hpp"
#include "td/utils/port/signals.h"
#include "td/utils/OptionParser.h"
#include "td/utils/filesystem.h"
#include "common/delay.h"
#include <fstream>
#include "overlay/overlays.h"

#include "auto/tl/ton_api_json.h"
#include "common/errorcode.h"

#include "tonlib/tonlib/TonlibClient.h"

#include "adnl/adnl.h"
#include "dht/dht.h"

#include <algorithm>
#include "td/utils/port/path.h"
#include "td/utils/JsonBuilder.h"
#include "auto/tl/ton_api_json.h"
#include "tl/tl_json.h"

#include "git.h"

using namespace ton;

td::IPAddress ip_addr;
std::string global_config;

class TelemetryCollector : public td::actor::Actor {
 public:
  TelemetryCollector() = default;

  td::Status load_global_config() {
    TRY_RESULT_PREFIX(conf_data, td::read_file(global_config), "failed to read: ");
    TRY_RESULT_PREFIX(conf_json, td::json_decode(conf_data.as_slice()), "failed to parse json: ");
    ton_api::config_global conf;
    TRY_STATUS_PREFIX(ton_api::from_json(conf, conf_json.get_object()), "json does not fit TL scheme: ");
    if (!conf.dht_) {
      return td::Status::Error(ErrorCode::error, "does not contain [dht] section");
    }
    TRY_RESULT_PREFIX(dht, dht::Dht::create_global_config(std::move(conf.dht_)), "bad [dht] section: ");
    dht_config_ = std::move(dht);
    zerostate_hash_ = conf.validator_->zero_state_->file_hash_;
    return td::Status::OK();
  }

  void run() {
    keyring_ = keyring::Keyring::create("");
    load_global_config().ensure();

    adnl_network_manager_ = adnl::AdnlNetworkManager::create(0);
    adnl_ = adnl::Adnl::create("", keyring_.get());
    td::actor::send_closure(adnl_, &adnl::Adnl::register_network_manager, adnl_network_manager_.get());
    adnl::AdnlCategoryMask cat_mask;
    cat_mask[0] = true;
    td::actor::send_closure(adnl_network_manager_, &adnl::AdnlNetworkManager::add_self_addr, ip_addr,
                            std::move(cat_mask), 0);
    addr_list_.set_version(static_cast<td::int32>(td::Clocks::system()));
    addr_list_.set_reinit_date(adnl::Adnl::adnl_start_time());
    addr_list_.add_udp_address(ip_addr);
    {
      auto pk = PrivateKey{privkeys::Ed25519::random()};
      auto pub = pk.compute_public_key();
      td::actor::send_closure(keyring_, &keyring::Keyring::add_key, std::move(pk), true, [](td::Unit) {});
      dht_id_ = adnl::AdnlNodeIdShort{pub.compute_short_id()};
      td::actor::send_closure(adnl_, &adnl::Adnl::add_id, adnl::AdnlNodeIdFull{pub}, addr_list_,
                              static_cast<td::uint8>(0));
    }
    {
      auto pk = PrivateKey{privkeys::Ed25519::random()};
      auto pub = pk.compute_public_key();
      td::actor::send_closure(keyring_, &keyring::Keyring::add_key, std::move(pk), true, [](td::Unit) {});
      local_id_ = adnl::AdnlNodeIdShort{pub.compute_short_id()};
      td::actor::send_closure(adnl_, &adnl::Adnl::add_id, adnl::AdnlNodeIdFull{pub}, addr_list_,
                              static_cast<td::uint8>(0));
    }
    auto D = dht::Dht::create_client(dht_id_, "", dht_config_, keyring_.get(), adnl_.get());
    D.ensure();
    dht_ = D.move_as_ok();
    td::actor::send_closure(adnl_, &adnl::Adnl::register_dht_node, dht_.get());

    overlays_ = overlay::Overlays::create("", keyring_.get(), adnl_.get(), dht_.get());

    class Callback : public overlay::Overlays::Callback {
     public:
      explicit Callback(td::actor::ActorId<TelemetryCollector> id) : id_(id) {
      }
      void receive_message(adnl::AdnlNodeIdShort src, overlay::OverlayIdShort overlay_id,
                           td::BufferSlice data) override {
      }
      void receive_query(adnl::AdnlNodeIdShort src, overlay::OverlayIdShort overlay_id, td::BufferSlice data,
                         td::Promise<td::BufferSlice> promise) override {
      }
      void receive_broadcast(PublicKeyHash src, overlay::OverlayIdShort overlay_id, td::BufferSlice data) override {
        td::actor::send_closure(id_, &TelemetryCollector::receive_broadcast, src, std::move(data));
      }
      void check_broadcast(PublicKeyHash src, overlay::OverlayIdShort overlay_id, td::BufferSlice data,
                           td::Promise<td::Unit> promise) override {
      }

     private:
      td::actor::ActorId<TelemetryCollector> id_;
    };

    auto X = create_hash_tl_object<ton_api::validator_telemetryOverlayId>(zerostate_hash_);
    td::BufferSlice b{32};
    b.as_slice().copy_from(as_slice(X));
    overlay::OverlayIdFull overlay_id_full{std::move(b)};
    overlay::OverlayPrivacyRules rules{
        8192, overlay::CertificateFlags::AllowFec | overlay::CertificateFlags::Trusted, {}};
    overlay::OverlayOptions opts;
    opts.frequent_dht_lookup_ = true;
    LOG(WARNING) << "Overlay id : " << overlay_id_full.compute_short_id();
    td::actor::send_closure(overlays_, &overlay::Overlays::create_public_overlay_ex, local_id_,
                            std::move(overlay_id_full), std::make_unique<Callback>(actor_id(this)), std::move(rules),
                            R"({ "type": "telemetry" })", opts);
  }

  void receive_broadcast(PublicKeyHash src, td::BufferSlice data) {
    auto R = fetch_tl_prefix<ton_api::validator_telemetry>(data, true);
    if (R.is_error()) {
      LOG(INFO) << "Invalid broadcast from " << src << ": " << R.move_as_error();
      return;
    }
    auto telemetry = R.move_as_ok();
    if (telemetry->adnl_id_ != src.bits256_value()) {
      LOG(INFO) << "Invalid broadcast from " << src << ": adnl_id mismatch";
      return;
    }
    auto s = td::json_encode<std::string>(td::ToJson(*telemetry), false);
    s.erase(std::remove_if(s.begin(), s.end(), [](char c) { return c == '\n' || c == '\r'; }), s.end());
    std::cout << s << "\n";
    std::cout.flush();
  }

 private:
  adnl::AdnlNodeIdShort dht_id_, local_id_;
  adnl::AdnlAddressList addr_list_;

  td::actor::ActorOwn<keyring::Keyring> keyring_;
  td::actor::ActorOwn<adnl::AdnlNetworkManager> adnl_network_manager_;
  td::actor::ActorOwn<adnl::Adnl> adnl_;
  td::actor::ActorOwn<dht::Dht> dht_;
  td::actor::ActorOwn<overlay::Overlays> overlays_;

  std::shared_ptr<dht::DhtGlobalConfig> dht_config_;
  td::Bits256 zerostate_hash_;
};

int main(int argc, char* argv[]) {
  SET_VERBOSITY_LEVEL(verbosity_INFO);

  td::set_default_failure_signal_handler().ensure();

  td::actor::ActorOwn<TelemetryCollector> x;
  td::unique_ptr<td::LogInterface> logger_;
  SCOPE_EXIT {
    td::log_interface = td::default_log_interface;
  };

  td::OptionParser p;
  p.set_description("collect validator telemetry from the overlay, print as json to stdout\n");
  p.add_option('v', "verbosity", "set verbosity level", [&](td::Slice arg) {
    int v = VERBOSITY_NAME(FATAL) + (td::to_integer<int>(arg));
    SET_VERBOSITY_LEVEL(v);
  });
  p.add_option('h', "help", "prints a help message", [&]() {
    char b[10240];
    td::StringBuilder sb(td::MutableSlice{b, 10000});
    sb << p;
    std::cout << sb.as_cslice().c_str();
    std::exit(2);
  });
  p.add_option('V', "version", "shows build information", [&]() {
    std::cout << "telemetyr-collector build information: [ Commit: " << GitMetadata::CommitSHA1()
              << ", Date: " << GitMetadata::CommitDate() << "]\n";
    std::exit(0);
  });
  p.add_option('C', "global-config", "global TON configuration file",
               [&](td::Slice arg) { global_config = arg.str(); });
  p.add_checked_option('a', "addr", "ip:port", [&](td::Slice arg) {
    TRY_STATUS(ip_addr.init_host_port(arg.str()));
    return td::Status::OK();
  });

  td::actor::Scheduler scheduler({3});

  scheduler.run_in_context([&] { x = td::actor::create_actor<TelemetryCollector>("collector"); });
  scheduler.run_in_context([&] { p.run(argc, argv).ensure(); });
  scheduler.run_in_context([&] { td::actor::send_closure(x, &TelemetryCollector::run); });
  while (scheduler.run(1)) {
  }

  return 0;
}
