terraform {
  required_providers {
    azurerm = {
      source  = "hashicorp/azurerm"
      version = ">= 3.7.0"
    }
  }

  backend "azurerm" {
    use_oidc = true
  }

}

provider "azurerm" {
  features {
    key_vault {
      purge_soft_delete_on_destroy = true
    }
  }
}



module "dataspace" {
  source                        = "./modules/dataspace"
  application_sp_client_id      = var.application_sp_client_id
  application_sp_object_id      = var.application_sp_object_id
  application_sp_client_secret  = var.application_sp_client_secret
  dataspace_authority_country   = "ES"
  prefix                        = var.res_prefix
  resource_group                = "rg-${var.res_prefix}-dataspace"
  public_key_jwk_file_authority = "${path.module}/generated/dataspace/authoritykey.public.jwk"
  public_key_jwk_file_gaiax     = "${path.module}/generated/dataspace/gaiaxkey.public.jwk"
  private_key_pem_file          = "${path.module}/generated/dataspace/authoritykey.pem"
}

# todo: iterate over #{var.participants}
module "participant1" {
  source                   = "./modules/participant"
  prefix                   = var.res_prefix
  participant_name         = "lieferant1"
  participant_region       = "eu"
  participant_country      = "FR"
  resource_group           = "rg-${var.res_prefix}-lieferant1"
  application_sp_client_id = var.application_sp_client_id
  application_sp_object_id = var.application_sp_object_id
  public_key_jwk_file      = "${path.module}/generated/lieferant1/participant.public.jwk"
  private_key_pem_file     = "${path.module}/generated/lieferant1/participant.pem"
}

module "participant2" {
  source                   = "./modules/participant"
  prefix                   = var.res_prefix
  participant_name         = "lieferant2"
  participant_region       = "eu"
  participant_country      = "DE"
  resource_group           = "rg-${var.res_prefix}-lieferant2"
  application_sp_client_id = var.application_sp_client_id
  application_sp_object_id = var.application_sp_object_id
  public_key_jwk_file      = "${path.module}/generated/lieferant2/participant.public.jwk"
  private_key_pem_file     = "${path.module}/generated/lieferant2/participant.pem"
}

module "participant3" {
  source                   = "./modules/participant"
  prefix                   = var.res_prefix
  participant_name         = "vnb"
  participant_region       = "us"
  participant_country      = "US"
  resource_group           = "rg-${var.res_prefix}-vnb"
  application_sp_client_id = var.application_sp_client_id
  application_sp_object_id = var.application_sp_object_id
  public_key_jwk_file      = "${path.module}/generated/vnb/participant.public.jwk"
  private_key_pem_file     = "${path.module}/generated/vnb/participant.pem"
}