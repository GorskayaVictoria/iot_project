require "sidekiq/web"
require "sidekiq/cron/web"

Sidekiq::Web.use ActionDispatch::Cookies
Sidekiq::Web.use ActionDispatch::Session::CookieStore, key: "_interslice_session"

Rails.application.routes.draw do
  mount Sidekiq::Web => "/sidekiq"

  resources :sensors, only: :index
end
